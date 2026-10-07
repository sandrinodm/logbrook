//! Command results use stdout; tracing diagnostics use stderr.
//!
//! A reader closing a pipe is successful completion of output. Other write
//! failures must propagate so a truncated redirect cannot report success.

use std::io::{self, Write};

pub fn line(arguments: std::fmt::Arguments<'_>) -> io::Result<()> {
    write_line(&mut io::stdout().lock(), arguments)
}

fn write_line(writer: &mut impl Write, arguments: std::fmt::Arguments<'_>) -> io::Result<()> {
    match writeln!(writer, "{arguments}").and_then(|()| writer.flush()) {
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingWriter {
        kind: io::ErrorKind,
        fail_on_flush: bool,
    }

    impl Write for FailingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.fail_on_flush {
                Ok(bytes.len())
            } else {
                Err(io::Error::from(self.kind))
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::from(self.kind))
        }
    }

    #[test]
    fn closed_pipe_is_success_but_other_write_and_flush_failures_propagate() {
        for fail_on_flush in [false, true] {
            for kind in [io::ErrorKind::BrokenPipe, io::ErrorKind::PermissionDenied] {
                let result = write_line(
                    &mut FailingWriter {
                        kind,
                        fail_on_flush,
                    },
                    format_args!("{}", serde_json::json!({"imported": 2})),
                );

                if kind == io::ErrorKind::BrokenPipe {
                    assert!(result.is_ok());
                } else {
                    assert_eq!(result.unwrap_err().kind(), kind);
                }
            }
        }
    }
}
