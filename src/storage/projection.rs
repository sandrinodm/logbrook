//! Immutable index projection mapping and the shared exact string derivation.
#[cfg(test)]
use super::{NormalizedEvent, Storage, StorageConfig, migrate};
use super::{Result, db_error, quote};
use crate::model::{Error, ErrorKind};
use duckdb::{Connection, params};
use std::result::Result as StdResult;

/// Original event columns in their durable row order.
pub(super) const ORIGINAL_COLUMNS: &str =
    "id,source,event_time,received_at,level,service,logger,host,pid,message,attributes";

/// Durable schema version that records the promoted string mapping.
const PROJECTION_SCHEMA_VERSION: i32 = 4;

#[derive(Clone, Debug)]
pub(super) struct ProjectionLayout {
    keys: Vec<String>,
}

fn integrity(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Integrity, message)
}

fn invalid_projection_keys() -> Error {
    integrity("invalid projection keys")
}

// This checks accidental schema drift, not hostile rewriting of both schema
// and manifest. Quoted key bytes remain exact, including doubled SQL quotes.
fn normalized_check(sql: &str) -> String {
    let mut literal = false;
    let mut normalized = String::new();
    for character in sql.chars() {
        if character == '\'' {
            literal = !literal;
        }
        let is_layout_token = !character.is_whitespace() && !matches!(character, '(' | ')');
        if literal || is_layout_token {
            normalized.push(character);
        }
    }
    normalized
}

// CachedStatement retains its last native result. Restrict this helper to the
// small, fixed layout/catalog SELECTs below; event and archive reads stay uncached.
enum LayoutStatement<'conn> {
    Plain(duckdb::Statement<'conn>),
    Cached(duckdb::CachedStatement<'conn>),
}

impl<'conn> std::ops::Deref for LayoutStatement<'conn> {
    type Target = duckdb::Statement<'conn>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Plain(statement) => statement,
            Self::Cached(statement) => statement,
        }
    }
}

impl std::ops::DerefMut for LayoutStatement<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Plain(statement) => statement,
            Self::Cached(statement) => statement,
        }
    }
}

/// Whether fixed layout lookups reuse this connection's bounded metadata cache.
#[derive(Clone, Copy)]
enum LayoutPreparation {
    Plain,
    Cached,
}

impl LayoutPreparation {
    fn prepare<'conn>(self, conn: &'conn Connection, sql: &str) -> Result<LayoutStatement<'conn>> {
        match self {
            Self::Plain => conn.prepare(sql).map(LayoutStatement::Plain),
            Self::Cached => conn.prepare_cached(sql).map(LayoutStatement::Cached),
        }
        .map_err(db_error)
    }
}

impl ProjectionLayout {
    pub(super) fn load(conn: &Connection) -> Result<Option<Self>> {
        Self::load_with_preparation(conn, LayoutPreparation::Plain)
    }

    pub(super) fn load_cached(conn: &Connection) -> Result<Option<Self>> {
        Self::load_with_preparation(conn, LayoutPreparation::Cached)
    }

    fn load_with_preparation(
        conn: &Connection,
        preparation: LayoutPreparation,
    ) -> Result<Option<Self>> {
        let version: i32 = preparation
            .prepare(conn, "SELECT max(version) FROM schema_version")?
            .query_row([], |r| r.get(0))
            .map_err(db_error)?;
        let present: i64 = preparation
            .prepare(
                conn,
                "SELECT count(*) FROM information_schema.tables \
                 WHERE table_schema='main' AND table_name='projection_layout'",
            )?
            .query_row([], |r| r.get(0))
            .map_err(db_error)?;
        if version < PROJECTION_SCHEMA_VERSION {
            if present != 0 {
                return Err(integrity("projection mapping on legacy schema"));
            }
            return Ok(None);
        }

        if version != PROJECTION_SCHEMA_VERSION || present != 1 {
            return Err(integrity("missing or unsupported projection mapping"));
        }

        let mut stmt = preparation.prepare(
            conn,
            "SELECT singleton,layout_version,keys_json FROM projection_layout",
        )?;
        let entries = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i32>(0)?,
                    r.get::<_, i32>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(db_error)?
            .collect::<StdResult<Vec<_>, _>>()
            .map_err(db_error)?;
        if entries.len() != 1 || entries[0].0 != 1 || entries[0].1 != 1 {
            return Err(integrity("invalid projection mapping singleton/version"));
        }

        let keys: Vec<String> = serde_json::from_str(&entries[0].2)
            .map_err(|error| invalid_projection_keys().with_source(error))?;
        crate::config::validate_promoted_string_keys(&keys)
            .map_err(|error| invalid_projection_keys().with_source(error))?;
        if keys.is_empty() || keys.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(integrity("projection keys must be nonempty and canonical"));
        }

        let layout = Self { keys };
        layout.validate_hot_schema(conn, preparation)?;
        Ok(Some(layout))
    }

    fn validate_hot_schema(&self, conn: &Connection, preparation: LayoutPreparation) -> Result<()> {
        self.validate_hot_columns(conn, preparation)?;
        self.validate_hot_constraints(conn, preparation)
    }

    fn validate_hot_columns(
        &self,
        conn: &Connection,
        preparation: LayoutPreparation,
    ) -> Result<()> {
        let mut stmt = preparation.prepare(
            conn,
            "SELECT column_name,data_type,is_nullable FROM information_schema.columns \
                 WHERE table_schema='main' AND table_name='events' ORDER BY ordinal_position",
        )?;
        let columns = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(db_error)?
            .collect::<StdResult<Vec<_>, _>>()
            .map_err(db_error)?;

        let mut expected = vec![
            ("id".into(), "BIGINT".into(), "NO".into()),
            ("source".into(), "VARCHAR".into(), "NO".into()),
            ("event_time".into(), "BIGINT".into(), "NO".into()),
            ("received_at".into(), "BIGINT".into(), "NO".into()),
            ("level".into(), "INTEGER".into(), "NO".into()),
            ("service".into(), "VARCHAR".into(), "YES".into()),
            ("logger".into(), "VARCHAR".into(), "YES".into()),
            ("host".into(), "VARCHAR".into(), "YES".into()),
            ("pid".into(), "BIGINT".into(), "YES".into()),
            ("message".into(), "VARCHAR".into(), "NO".into()),
            ("attributes".into(), "VARCHAR".into(), "NO".into()),
            ("pa_covered".into(), "BOOLEAN".into(), "NO".into()),
        ];
        expected.extend(
            self.slot_names()
                .into_iter()
                .map(|name| (name, "VARCHAR".into(), "YES".into())),
        );

        if columns != expected {
            return Err(integrity("hot projection column layout mismatch"));
        }
        Ok(())
    }

    fn validate_hot_constraints(
        &self,
        conn: &Connection,
        preparation: LayoutPreparation,
    ) -> Result<()> {
        let mut stmt = preparation.prepare(
            conn,
            "SELECT expression FROM duckdb_constraints() \
                 WHERE schema_name='main' AND table_name='events' AND constraint_type='CHECK'",
        )?;
        let checks = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(db_error)?
            .collect::<StdResult<Vec<_>, _>>()
            .map_err(db_error)?;
        // The catalog adds display parentheses and whitespace. Compare all other
        // tokens, preserving string literals, to catch mapping/schema drift.
        if checks.len() != 1
            || normalized_check(&checks[0])
                != normalized_check(&self.covered_validation_sql("attributes"))
        {
            return Err(integrity(
                "hot projection integrity constraint/layout mismatch",
            ));
        }

        let primary_keys: i64 = preparation
            .prepare(
                conn,
                "SELECT count(*) FROM duckdb_constraints() \
                 WHERE schema_name='main' AND table_name='events' \
                 AND constraint_type='PRIMARY KEY' AND constraint_column_names=['id']",
            )?
            .query_row([], |r| r.get(0))
            .map_err(db_error)?;
        if primary_keys != 1 {
            return Err(integrity("missing hot event primary key"));
        }
        Ok(())
    }

    pub(super) fn keys(&self) -> &[String] {
        &self.keys
    }

    pub(super) fn slot_names(&self) -> Vec<String> {
        (0..self.keys.len())
            .map(|index| format!("pa_{index}"))
            .collect()
    }

    pub(super) fn extra_column_names(&self) -> Vec<String> {
        std::iter::once("pa_covered".into())
            .chain(self.slot_names())
            .collect()
    }

    pub(super) fn derivation_sql(&self, attributes: &str, slot: usize) -> String {
        let pointer = format!("/{}", self.keys[slot].replace('~', "~0").replace('/', "~1"));
        let path = if pointer.contains('\0') {
            let hex = pointer
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            format!("decode(from_hex('{hex}'))")
        } else {
            format!("'{}'", quote(&pointer))
        };
        format!(
            "CASE WHEN json_type({attributes})='OBJECT' \
             AND json_type({attributes},{path})='VARCHAR' \
             THEN json_extract_string({attributes},{path}) ELSE NULL END"
        )
    }

    pub(super) fn covered_validation_sql(&self, attributes: &str) -> String {
        let nulls = self
            .slot_names()
            .iter()
            .map(|slot| format!("{slot} IS NULL"))
            .collect::<Vec<_>>()
            .join(" AND ");

        let equal = self
            .slot_names()
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                format!(
                    "({slot} IS NOT DISTINCT FROM ({}))",
                    self.derivation_sql(attributes, i)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        format!(
            "CASE WHEN NOT pa_covered THEN {nulls} \
             WHEN json_valid({attributes}) THEN {equal} \
             ELSE CAST('f' AS BOOLEAN) END"
        )
    }

    pub(super) fn backfill_columns_sql(&self, attributes: &str) -> String {
        let mut fields = vec![format!("json_valid({attributes}) AS pa_covered")];
        fields.extend(self.slot_names().iter().enumerate().map(|(i, slot)| {
            format!(
                "CASE WHEN json_valid({attributes}) THEN ({}) ELSE NULL END AS {slot}",
                self.derivation_sql(attributes, i)
            )
        }));
        fields.join(",")
    }

    pub(super) fn take_values(&self, value: &mut serde_json::Value) -> Vec<Option<String>> {
        self.keys
            .iter()
            .map(|key| {
                value
                    .as_object_mut()
                    .and_then(|object| object.remove(key))
                    .and_then(|value| match value {
                        serde_json::Value::String(text) => Some(text),
                        _ => None,
                    })
            })
            .collect()
    }

    pub(super) fn retained_bytes(&self, value: &serde_json::Value) -> usize {
        self.keys
            .iter()
            .filter_map(|key| value.as_object().and_then(|object| object.get(key)))
            .map(|value| match value {
                serde_json::Value::String(text) => text.capacity(),
                _ => 0,
            })
            .sum::<usize>()
            + self.keys.len() * std::mem::size_of::<Option<String>>()
    }
}

pub(super) fn activate(
    conn: &mut Connection,
    configured: &[String],
) -> Result<Option<ProjectionLayout>> {
    crate::config::validate_promoted_string_keys(configured)?;
    let mut keys = configured.to_vec();
    keys.sort();
    if let Some(layout) = ProjectionLayout::load(conn)? {
        if !keys.is_empty() && keys != layout.keys {
            return Err(Error::invalid(
                "promoted_string_keys differs from the durable index layout; \
                 create/reimport an index to change promoted keys",
            ));
        }
        return Ok(Some(layout));
    }

    if keys.is_empty() {
        return Ok(None);
    }

    let layout = ProjectionLayout { keys };
    let tx = conn.transaction().map_err(db_error)?;
    let slots = layout
        .slot_names()
        .iter()
        .map(|slot| format!(",{slot} VARCHAR"))
        .collect::<String>();
    tx.execute_batch(&format!(
        "CREATE TABLE events_projected(id BIGINT PRIMARY KEY,source VARCHAR NOT NULL,\
         event_time BIGINT NOT NULL,received_at BIGINT NOT NULL,level INTEGER NOT NULL,\
         service VARCHAR,logger VARCHAR,host VARCHAR,pid BIGINT,message VARCHAR NOT NULL,\
         attributes VARCHAR NOT NULL,pa_covered BOOLEAN NOT NULL{slots},CHECK ({})); \
         INSERT INTO events_projected SELECT {ORIGINAL_COLUMNS},FALSE,{} FROM events; \
         DROP TABLE events; ALTER TABLE events_projected RENAME TO events; \
         CREATE TABLE projection_layout(singleton INTEGER PRIMARY KEY CHECK(singleton=1),\
         layout_version INTEGER NOT NULL,keys_json VARCHAR NOT NULL); \
         ALTER TABLE archives ADD COLUMN projection_sha256 VARCHAR; \
         INSERT INTO schema_version VALUES(4);",
        layout.covered_validation_sql("attributes"),
        vec!["NULL"; layout.keys.len()].join(",")
    ))
    .map_err(db_error)?;

    tx.execute(
        "INSERT INTO projection_layout VALUES(1,1,?)",
        params![serde_json::to_string(&layout.keys).map_err(db_error)?],
    )
    .map_err(db_error)?;

    tx.commit().map_err(db_error)?;

    ProjectionLayout::load(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured() -> (Connection, ProjectionLayout) {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        let layout = activate(&mut conn, &["runId".into(), "a/~'雪".into()])
            .unwrap()
            .unwrap();
        (conn, layout)
    }

    #[test]
    fn activation_preserves_malformed_legacy_and_enforces_exact_slots() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        conn.execute(
            "INSERT INTO events VALUES(1,'s',1,1,2,NULL,NULL,NULL,NULL,'m','invalid')",
            [],
        )
        .unwrap();

        let layout = activate(&mut conn, &["runId".into()]).unwrap().unwrap();
        let covered: bool = conn
            .query_row("SELECT pa_covered FROM events", [], |r| r.get(0))
            .unwrap();
        assert!(!covered);

        assert!(
            conn.execute("UPDATE events SET pa_0='stale' WHERE id=1", [])
                .is_err()
        );
        assert!(
            conn.execute("UPDATE events SET pa_covered=TRUE WHERE id=1", [])
                .is_err()
        );

        conn.execute(
            "INSERT INTO events VALUES(2,'s',1,1,2,NULL,NULL,NULL,NULL,'m',\
             '{\"runId\":\"x\"}',TRUE,'x')",
            [],
        )
        .unwrap();

        for update in [
            "pa_0=NULL",
            "pa_0='stale'",
            "attributes='invalid'",
            "pa_covered=FALSE",
        ] {
            assert!(
                conn.execute(&format!("UPDATE events SET {update} WHERE id=2"), [])
                    .is_err(),
                "{update}"
            );
        }

        assert_eq!(layout.keys(), &["runId"]);
    }

    #[test]
    fn literal_keys_and_nonstring_roots_follow_decoded_json() {
        let (mut conn, layout) = configured();
        let values = [
            serde_json::json!({"runId":"hello雪", "a/~'雪":"escaped"}),
            serde_json::json!({"runId":null}),
            serde_json::json!({"runId":3}),
            serde_json::json!({"runId":true}),
            serde_json::json!(["x"]),
            serde_json::json!("root"),
            serde_json::json!({}),
        ];
        for (index, value) in values.iter().enumerate() {
            let mut value = value.clone();
            let json = value.to_string();
            let projected = layout.take_values(&mut value);
            conn.execute(
                "INSERT INTO events VALUES(?,'s',1,1,2,NULL,NULL,NULL,NULL,'m',?,TRUE,?,?)",
                params![index as i64, json, projected[0], projected[1]],
            )
            .unwrap();
        }

        assert!(ProjectionLayout::load(&conn).unwrap().is_some());
        assert!(activate(&mut conn, &[]).unwrap().is_some());
        assert!(
            activate(&mut conn, &["runId".into()])
                .unwrap_err()
                .message
                .contains("create/reimport")
        );
    }

    #[test]
    fn nul_key_uses_an_exact_constant_expression() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        let layout = activate(&mut conn, &["nul\0key".into()]).unwrap().unwrap();
        let value = serde_json::json!({"nul\0key":"value"}).to_string();
        conn.execute(
            "INSERT INTO events VALUES(1,'s',1,1,2,NULL,NULL,NULL,NULL,'m',?,TRUE,'value')",
            params![value],
        )
        .unwrap();

        let derived: String = conn
            .query_row(
                &format!(
                    "SELECT {} FROM events",
                    layout.derivation_sql("attributes", 0)
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(derived, "value");
    }

    #[test]
    fn cached_layout_reads_refresh_after_activation_and_detect_drift() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        conn.set_prepared_statement_cache_capacity(32);
        assert!(ProjectionLayout::load_cached(&conn).unwrap().is_none());
        assert!(ProjectionLayout::load_cached(&conn).unwrap().is_none());

        let mut writer = conn.try_clone().unwrap();
        activate(&mut writer, &["runId".into()]).unwrap();
        for _ in 0..2 {
            assert_eq!(
                ProjectionLayout::load_cached(&conn)
                    .unwrap()
                    .unwrap()
                    .keys(),
                ["runId"]
            );
        }

        writer
            .execute("UPDATE projection_layout SET keys_json='[\"changed\"]'", [])
            .unwrap();

        assert_eq!(
            ProjectionLayout::load_cached(&conn).unwrap_err().kind,
            ErrorKind::Integrity
        );

        writer
            .execute("UPDATE projection_layout SET keys_json='[\"runId\"]'", [])
            .unwrap();

        assert!(ProjectionLayout::load_cached(&conn).unwrap().is_some());

        writer
            .execute_batch("ALTER TABLE events ADD COLUMN unexpected VARCHAR")
            .unwrap();

        assert_eq!(
            ProjectionLayout::load_cached(&conn).unwrap_err().kind,
            ErrorKind::Integrity
        );
    }

    #[test]
    fn durable_mapping_cannot_drift_from_hot_constraint() {
        let (conn, _) = configured();
        conn.execute(
            "UPDATE projection_layout SET keys_json='[\"changed\",\"runId\"]'",
            [],
        )
        .unwrap();
        assert_eq!(
            ProjectionLayout::load(&conn).unwrap_err().kind,
            ErrorKind::Integrity
        );
    }

    #[tokio::test]
    async fn retained_projected_strings_are_reserved_in_queue_bytes() {
        let dir = tempfile::tempdir().unwrap();
        for promoted in [false, true] {
            let storage = Storage::open(StorageConfig {
                data_dir: dir.path().join(if promoted { "on" } else { "off" }),
                queue_bytes: 1024,
                promoted_string_keys: if promoted {
                    vec!["runId".into()]
                } else {
                    vec![]
                },
                ..StorageConfig::default()
            })
            .unwrap();

            let event = NormalizedEvent {
                event_time: 10,
                level: 30,
                service: None,
                logger: None,
                host: None,
                pid: None,
                message: "m".into(),
                attributes: serde_json::json!({"runId":"x".repeat(500)}),
            };
            let result = storage.append("s".into(), vec![event]).await;
            if promoted {
                assert_eq!(result.unwrap_err().kind, ErrorKind::TooLarge);
            } else {
                assert_eq!(result.unwrap(), 1);
            }

            storage.shutdown().await.unwrap();
        }
    }

    #[test]
    fn direct_open_validates_keys_before_creating_storage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent");
        let result = Storage::open(StorageConfig {
            data_dir: path.clone(),
            promoted_string_keys: vec!["x".into(), "x".into()],
            ..StorageConfig::default()
        });
        assert!(result.is_err());
        assert!(!path.exists());
    }

    #[test]
    fn version_one_and_two_upgrade_without_parsing_legacy_attributes() {
        for version in [1, 2] {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(include_str!("../../migrations/001_initial.sql"))
                .unwrap();

            if version == 2 {
                conn.execute("INSERT INTO schema_version VALUES(2)", [])
                    .unwrap();
            }

            conn.execute(
                "INSERT INTO events VALUES(42,'s',1,1,2,NULL,NULL,NULL,NULL,'m','malformed')",
                [],
            )
            .unwrap();
            conn.execute("UPDATE ingest_state SET next_id=43", [])
                .unwrap();

            migrate(&mut conn).unwrap();
            activate(&mut conn, &["runId".into()]).unwrap();

            assert_eq!(
                conn.query_row("SELECT max(version) FROM schema_version", [], |r| r
                    .get::<_, i32>(0))
                    .unwrap(),
                4
            );
            assert_eq!(
                conn.query_row("SELECT next_id FROM ingest_state", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                43
            );
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM events WHERE id=42 AND NOT pa_covered \
                     AND pa_0 IS NULL AND attributes='malformed'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                1
            );
        }
    }

    #[test]
    fn activation_failure_rolls_back_original_schema_and_data() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn).unwrap();
        conn.execute(
            "INSERT INTO events VALUES(1,'s',1,1,2,NULL,NULL,NULL,NULL,'m','bad')",
            [],
        )
        .unwrap();

        conn.execute(
            "ALTER TABLE archives ADD COLUMN projection_sha256 VARCHAR",
            [],
        )
        .unwrap();

        assert!(activate(&mut conn, &["runId".into()]).is_err());

        assert_eq!(
            conn.query_row("SELECT max(version) FROM schema_version", [], |r| r
                .get::<_, i32>(0))
                .unwrap(),
            3
        );

        assert_eq!(
            conn.query_row("SELECT attributes FROM events", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "bad"
        );

        assert!(ProjectionLayout::load(&conn).unwrap().is_none());
    }
}
