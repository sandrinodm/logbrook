use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub const WRITE: &str = "synthetic-process-write-token";
pub const READ: &str = "synthetic-process-read-token";
pub const ADMIN: &str = "synthetic-process-admin-token";

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub struct Fixture {
    pub root: tempfile::TempDir,
    pub data: PathBuf,
    pub env: BTreeMap<String, String>,
    pub config: Option<PathBuf>,
    pub base: String,
    child: Option<Child>,
    client: reqwest::Client,
}

impl Fixture {
    pub fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let env = BTreeMap::from([
            ("LOGBROOK_DATA_DIR".into(), data.to_str().unwrap().into()),
            ("LOGBROOK_BIND".into(), address.to_string()),
            ("LOGBROOK_INGEST_TOKEN".into(), WRITE.into()),
            ("LOGBROOK_READ_TOKEN".into(), READ.into()),
            ("LOGBROOK_ADMIN_TOKEN".into(), ADMIN.into()),
        ]);

        Self {
            root,
            data,
            env,
            config: None,
            base: format!("http://{address}"),
            child: None,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_logbrook"));
        for (key, _) in
            std::env::vars_os().filter(|(key, _)| key.to_string_lossy().starts_with("LOGBROOK_"))
        {
            command.env_remove(key);
        }
        command.envs(&self.env);
        if let Some(config) = &self.config {
            command.arg("--config").arg(config);
        }
        command
    }

    pub fn cli(&self, args: &[&str], success: bool, remote: bool) -> String {
        let stdout = tempfile::tempfile().unwrap();
        let stderr = tempfile::tempfile().unwrap();
        let mut command = self.command();
        if remote {
            command.args(["--url", &self.base]);
        }
        let mut child = command
            .args(args)
            .stdout(stdout.try_clone().unwrap())
            .stderr(stderr.try_clone().unwrap())
            .spawn()
            .unwrap();
        let status = wait_child(&mut child, Duration::from_secs(60));
        use std::io::{Read, Seek, SeekFrom};
        let read = |mut file: File| {
            file.seek(SeekFrom::Start(0)).unwrap();
            let mut result = String::new();
            file.read_to_string(&mut result).unwrap();
            result
        };
        let output = read(stdout);
        let diagnostic = read(stderr);
        assert_eq!(status.success(), success, "{args:?}: {output}{diagnostic}");
        if !success {
            assert!(output.is_empty());
            for token in [WRITE, READ, ADMIN] {
                assert!(!diagnostic.contains(token));
            }
        }
        output
    }

    pub fn cli_json(&self, args: &[&str]) -> Value {
        let mut all = vec!["--json"];
        all.extend_from_slice(args);
        serde_json::from_str(&self.cli(&all, true, true)).unwrap()
    }

    pub async fn start(&mut self) {
        assert!(self.child.is_none());
        let log = File::options()
            .create(true)
            .append(true)
            .open(self.root.path().join("server.log"))
            .unwrap();
        self.child = Some(
            self.command()
                .arg("serve")
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(
                self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "server exited: {}",
                self.logs()
            );
            if let Ok(response) = self.client.get(format!("{}/ready", self.base)).send().await
                && response.status().is_success()
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "readiness timeout: {}",
                self.logs()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub fn stop(&mut self) {
        let mut child = self.child.take().unwrap();
        // SIGTERM exercises the production graceful shutdown path.
        let mut signal = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        assert!(wait_child(&mut signal, Duration::from_secs(5)).success());
        assert!(
            wait_child(&mut child, Duration::from_secs(35)).success(),
            "{}",
            self.logs()
        );
    }

    pub fn crash(&mut self) {
        let mut child = self.child.take().unwrap();
        child.kill().unwrap();
        assert!(!wait_child(&mut child, Duration::from_secs(10)).success());
    }

    fn logs(&self) -> String {
        fs::read_to_string(self.root.path().join("server.log")).unwrap_or_default()
    }

    pub async fn request(&self, path: &str, body: Option<&Value>, token: &str) -> (u16, Value) {
        let builder = if body.is_some() {
            self.client.post(format!("{}{path}", self.base))
        } else {
            self.client.get(format!("{}{path}", self.base))
        };
        let mut builder = builder.bearer_auth(token);
        if let Some(body) = body {
            builder = builder
                .header("Content-Type", "application/json")
                .body(serde_json::to_vec(body).unwrap());
        }
        let response = builder.send().await.unwrap();
        let status = response.status().as_u16();
        let bytes = response.bytes().await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    pub async fn ingest(&self, index: &str, events: Value) -> u16 {
        let (status, answer) = self
            .request(
                &format!("/indexes/{index}/logs/ingest"),
                Some(&events),
                WRITE,
            )
            .await;
        if status == 200 {
            assert_eq!(
                answer,
                json!({"accepted": events.as_array().unwrap().len()})
            );
        }
        status
    }

    pub async fn count(&self, index: &str, message: &str) -> u64 {
        let mut url =
            reqwest::Url::parse(&format!("{}/indexes/{index}/logs/count", self.base)).unwrap();
        url.query_pairs_mut()
            .append_pair("from", &(now() - 30 * 86_400_000).to_string())
            .append_pair("to", &(now() + 60000).to_string())
            .append_pair("message", message);
        let path = format!("{}?{}", url.path(), url.query().unwrap());
        let (status, answer) = self.request(&path, None, READ).await;
        assert_eq!(status, 200, "{answer}");
        answer["count"].as_u64().unwrap()
    }

    pub async fn metrics(&self) -> BTreeMap<String, f64> {
        let response = self
            .client
            .get(format!("{}/metrics", self.base))
            .bearer_auth(READ)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        response
            .text()
            .await
            .unwrap()
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| {
                let mut parts = line.split_whitespace();
                (
                    parts.next().unwrap().into(),
                    parts.next().unwrap().parse().unwrap(),
                )
            })
            .collect()
    }

    pub async fn wait_metric(&mut self, name: &str, index: &str, predicate: impl Fn(f64) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(25);
        loop {
            assert!(
                self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "{}",
                self.logs()
            );
            if predicate(metric(&self.metrics().await, name, index)) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {name}: {}",
                self.logs()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

pub fn metric(metrics: &BTreeMap<String, f64>, name: &str, index: &str) -> f64 {
    metrics[&format!("{name}{{index=\"{index}\"}}")]
}

fn wait_child(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child process exit timed out");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // A failed assertion must never leave the fixture server running.
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
