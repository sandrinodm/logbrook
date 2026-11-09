//! Live tail over Server-Sent Events: replays retained history from `Last-Event-ID`, then follows
//! new commits, bounded by a per-subscriber byte budget.

use super::{
    App, Arc, Duration, Error, ErrorKind, Extension, HeaderMap, IntoResponse, KeepAlive, Ordering,
    OwnedSemaphorePermit, Params, Query, QueryRejection, Response, Sse, SseEvent, filters,
    overload, query_error, query_params,
};

use crate::{
    model::SearchResult,
    query_cursor::{TailCursor, decode_tail},
};
use std::collections::VecDeque;
use tokio::sync::watch;

/// Bytes an SSE frame adds besides its event name, id and data: `event: ` with its line break,
/// `data: ` with its line break, and the blank line that ends the frame.
const SSE_FRAME_OVERHEAD_BYTES: usize = 16;

/// Bytes an SSE id line adds besides the id: `id: ` and its line break.
const SSE_ID_OVERHEAD_BYTES: usize = 5;

/// Bytes each embedded line break in the data adds: a new `data: ` prefix on the continuation line.
const SSE_CONTINUATION_LINE_BYTES: usize = 6;

/// Panic message for a cursor the query already validated. Encoding it again cannot fail.
const VALIDATED_CURSOR: &str = "validated query cursor";

fn tail_cursor_expired(error: &Error) -> bool {
    error.kind == ErrorKind::CursorExpired
}

/// The bytes one SSE frame occupies on the wire, so the byte budget bounds the real output.
///
/// axum writes `event: <event>`, an optional `id: <id>`, one `data:` line per line of data, and a
/// blank line. JSON log data has no literal newlines, but the count stays correct if it does.
fn frame_bytes(event: &str, id: Option<&str>, data: &str) -> usize {
    let line_breaks = data
        .bytes()
        .filter(|byte| matches!(byte, b'\r' | b'\n'))
        .count();
    event.len()
        + SSE_FRAME_OVERHEAD_BYTES
        + id.map_or(0, |id| id.len() + SSE_ID_OVERHEAD_BYTES)
        + data.len()
        + line_breaks * SSE_CONTINUATION_LINE_BYTES
}

/// The final frame of a stream that cannot continue. It names the recovery the client should use.
fn terminal_frame(error: &Error, budget: usize) -> SseEvent {
    let gap = tail_cursor_expired(error) || error.kind == ErrorKind::TooLarge;
    let kind = if gap { "gap" } else { "error" };
    let message = if tail_cursor_expired(error) {
        "tail cursor expired by retention; search retained history before reconnecting"
    } else if error.kind == ErrorKind::TooLarge {
        "subscriber output byte budget exceeded; search retained history before reconnecting"
    } else {
        "tail query failed; reconnect or search retained history"
    };
    let end = message
        .len()
        .min(budget.saturating_sub(frame_bytes(kind, None, "")));
    SseEvent::default().event(kind).data(&message[..end])
}

/// The position a reconnecting client asks to resume from.
fn last_event_id(headers: &HeaderMap) -> Result<Option<String>, Error> {
    headers
        .get("last-event-id")
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()
        .map_err(|error| Error::invalid("invalid Last-Event-ID").with_source(error))
}

/// Streams matching events as Server-Sent Events.
///
/// # Errors
///
/// Fails before the stream starts when the filters or `Last-Event-ID` are invalid, the byte
/// budget cannot hold a frame, or the subscriber limit is reached.
pub(super) async fn tail(
    Extension(app): Extension<App>,
    headers: HeaderMap,
    filters: Result<Params<filters::TailFilters>, QueryRejection>,
) -> Result<Response, Error> {
    let query = filters::common_query(&app, &headers, query_params(filters)?.into())?;
    let after = last_event_id(&headers)?;
    // Decode before reserving subscriber or storage capacity. The storage boundary
    // repeats this validation for direct callers.
    if let Some(cursor) = &after {
        decode_tail(&query, cursor)?;
    }

    if app.config.max_tail_buffer_bytes < frame_bytes("error", None, "") {
        return Err(Error::new(
            ErrorKind::TooLarge,
            "tail byte budget is too small",
        ));
    }

    let permit = Arc::clone(&app.tails)
        .try_acquire_owned()
        .map_err(|error| {
            overload(&app, "tail_subscribers", "tail subscriber limit exhausted").with_source(error)
        })?;

    let cursor_codec = TailCursor::new(&query)?;
    let commits = app.storage.subscribe_commits();
    let first = app
        .storage
        .tail_filtered_bounded(after, query.clone(), app.config.max_tail_buffer_bytes)
        .await
        .map_err(|e| query_error(&app, e));
    if let Err(error) = &first
        && !tail_cursor_expired(error)
    {
        return Err(error.clone());
    }

    let state = TailStream {
        app,
        query,
        cursor_codec,
        result: first,
        _permit: permit,
        pending: VecDeque::new(),
        ended: false,
        wait: false,
        fetch: false,
        commits,
    };
    // Pull one bounded page at a time. A full replay page is drained immediately;
    // downstream demand is the backpressure signal and no backlog size implies loss.
    let stream = futures_util::stream::unfold(state, |mut state| async move {
        let frame = state.next_frame().await?;
        Some((Ok::<_, std::convert::Infallible>(frame), state))
    });
    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response())
}

/// The state of one live tail, advanced one SSE frame at a time.
struct TailStream {
    app: App,
    query: Query,
    cursor_codec: TailCursor,
    /// The latest storage page. Its `next_cursor` is where the next fetch resumes.
    result: Result<SearchResult, Error>,
    /// Holds one tail-subscriber slot for as long as the stream lives.
    _permit: OwnedSemaphorePermit,
    /// Frames already decided but not yet sent.
    pending: VecDeque<SseEvent>,
    ended: bool,
    /// The last page was empty, so wait for a commit before fetching again.
    wait: bool,
    /// Every queued frame has been sent, so the next page may be fetched.
    fetch: bool,
    commits: watch::Receiver<u64>,
}

impl TailStream {
    /// The next frame, or `None` when the stream is over.
    async fn next_frame(&mut self) -> Option<SseEvent> {
        if self.ended && self.pending.is_empty() {
            return None;
        }

        if self.pending.is_empty() && !self.refill().await {
            return None;
        }

        let next = self.pending.pop_front().expect("SSE frame");

        if self.pending.is_empty() && !self.ended {
            self.fetch = true;
        }

        Some(next)
    }

    /// Queues frames for the next page, waiting for a commit or fetching first when needed.
    /// Returns `false` when the storage is gone and the stream must end without a frame.
    async fn refill(&mut self) -> bool {
        if self.wait {
            tokio::select! {
                _ = self.commits.changed() => {},
                _ = tokio::time::sleep(Duration::from_millis(self.app.config.tail_poll_ms)) => {},
            }

            if !self.app.storage.is_ready() {
                return false;
            }
        }

        if self.fetch {
            let cursor = self
                .result
                .as_ref()
                .ok()
                .and_then(|page| page.next_cursor.clone());
            self.result = self
                .app
                .storage
                .tail_filtered_bounded(
                    cursor,
                    self.query.clone(),
                    self.app.config.max_tail_buffer_bytes,
                )
                .await
                .map_err(|error| query_error(&self.app, error));
        }

        let empty = SearchResult {
            events: vec![],
            next_cursor: None,
        };
        match std::mem::replace(&mut self.result, Ok(empty)) {
            Err(error) => self.end_with_error(&error),
            Ok(page) => {
                self.queue_page(&page);
                self.result = Ok(page);
            }
        }

        true
    }

    /// Ends the stream with a gap or error frame.
    fn end_with_error(&mut self, error: &Error) {
        self.ended = true;

        if tail_cursor_expired(error) || error.kind == ErrorKind::TooLarge {
            self.app.tail_gaps.fetch_add(1, Ordering::Relaxed);
        }

        self.pending
            .push_back(terminal_frame(error, self.app.config.max_tail_buffer_bytes));
    }

    /// Queues one event frame per event in the page, then a checkpoint when the page was
    /// empty. An event that would exceed the byte budget ends the stream instead.
    fn queue_page(&mut self, page: &SearchResult) {
        let budget = self.app.config.max_tail_buffer_bytes;
        let mut bytes = 0usize;

        for event in &page.events {
            let data = serde_json::to_string(event).expect("event serialization");
            let cursor = self
                .cursor_codec
                .encode(event.id.parse().expect("stored event ID"))
                .expect(VALIDATED_CURSOR);
            bytes = bytes.saturating_add(frame_bytes("log", Some(&cursor), &data));

            if bytes > budget {
                self.ended = true;
                self.app.tail_gaps.fetch_add(1, Ordering::Relaxed);
                self.pending.clear();
                self.pending.push_back(terminal_frame(
                    &Error::new(ErrorKind::TooLarge, "tail output exceeds budget"),
                    budget,
                ));
                break;
            }

            self.pending
                .push_back(SseEvent::default().event("log").id(cursor).data(data));
        }

        self.wait = page.events.is_empty();

        if self.pending.is_empty() {
            self.queue_checkpoint(page.next_cursor.clone());
        }
    }

    /// Queues a `caught up` cursor frame, or a gap frame when even that would not fit the budget.
    fn queue_checkpoint(&mut self, cursor: Option<String>) {
        let budget = self.app.config.max_tail_buffer_bytes;
        let cursor = cursor.unwrap_or_else(|| self.cursor_codec.encode(0).expect(VALIDATED_CURSOR));
        if frame_bytes("cursor", Some(&cursor), "caught up") > budget {
            self.ended = true;
            self.app.tail_gaps.fetch_add(1, Ordering::Relaxed);
            self.pending.push_back(terminal_frame(
                &Error::new(ErrorKind::TooLarge, "tail checkpoint exceeds budget"),
                budget,
            ));
        } else {
            self.pending.push_back(
                SseEvent::default()
                    .event("cursor")
                    .id(cursor)
                    .data("caught up"),
            );
        }
    }
}
