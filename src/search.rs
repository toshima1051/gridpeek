use std::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A single search hit: byte offset in the file
#[derive(Clone, Debug)]
pub struct SearchHit {
    pub byte_offset: usize,
    /// Length of the match (for regex matches that vary in length)
    pub match_len: Option<usize>,
}

/// Handle to a running search
pub struct SearchHandle {
    pub results: mpsc::Receiver<Vec<SearchHit>>,
    pub cancel: Arc<AtomicBool>,
}

/// Launch a background search on raw bytes.
/// Sends batches of results through the channel.
pub fn search_background(
    data: Arc<Vec<u8>>,
    query: String,
) -> SearchHandle {
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_clone = cancel.clone();
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        let query_bytes = query.as_bytes();
        if query_bytes.is_empty() {
            return;
        }

        let finder = memchr::memmem::Finder::new(query_bytes);
        let mut batch = Vec::with_capacity(256);
        let mut pos = 0;

        while pos < data.len() {
            if cancel_clone.load(Ordering::Relaxed) {
                return;
            }

            match finder.find(&data[pos..]) {
                Some(offset) => {
                    let abs_offset = pos + offset;
                    batch.push(SearchHit {
                        byte_offset: abs_offset,
                        match_len: None,
                    });
                    pos = abs_offset + 1;

                    if batch.len() >= 256 {
                        if tx.send(std::mem::take(&mut batch)).is_err() {
                            return;
                        }
                        batch = Vec::with_capacity(256);
                    }
                }
                None => break,
            }
        }

        // Send remaining results
        if !batch.is_empty() {
            let _ = tx.send(batch);
        }
    });

    SearchHandle {
        results: rx,
        cancel,
    }
}

/// Case-insensitive search variant
pub fn search_background_case_insensitive(
    data: Arc<Vec<u8>>,
    query: String,
) -> SearchHandle {
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_clone = cancel.clone();
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        let lower_query = query.to_lowercase();
        let query_bytes = lower_query.as_bytes();
        if query_bytes.is_empty() {
            return;
        }

        // For case-insensitive, we need to lowercase the data too.
        // Process in chunks to avoid allocating the entire file lowercased.
        let chunk_size = 64 * 1024 * 1024; // 64MB
        let query_len = query_bytes.len();
        let mut batch = Vec::with_capacity(256);
        let mut global_offset = 0;

        while global_offset < data.len() {
            if cancel_clone.load(Ordering::Relaxed) {
                return;
            }

            // Take a chunk with overlap to avoid missing matches at boundaries
            let chunk_end = (global_offset + chunk_size + query_len - 1).min(data.len());
            let chunk = &data[global_offset..chunk_end];

            // Lowercase the chunk
            let lower_chunk: Vec<u8> = chunk.iter().map(|b| b.to_ascii_lowercase()).collect();

            let finder = memchr::memmem::Finder::new(query_bytes);
            let search_end = (chunk_size).min(lower_chunk.len());

            let mut pos = 0;
            while pos < search_end {
                match finder.find(&lower_chunk[pos..]) {
                    Some(offset) => {
                        let abs_offset = global_offset + pos + offset;
                        batch.push(SearchHit {
                            byte_offset: abs_offset,
                            match_len: None,
                        });
                        pos += offset + 1;

                        if batch.len() >= 256 {
                            if tx.send(std::mem::take(&mut batch)).is_err() {
                                return;
                            }
                            batch = Vec::with_capacity(256);
                        }
                    }
                    None => break,
                }
            }

            global_offset += chunk_size;
        }

        if !batch.is_empty() {
            let _ = tx.send(batch);
        }
    });

    SearchHandle {
        results: rx,
        cancel,
    }
}

/// Regex search variant. Returns Err if regex fails to compile.
pub fn search_background_regex(
    data: Arc<Vec<u8>>,
    pattern: String,
    case_insensitive: bool,
) -> Result<SearchHandle, String> {
    let re = regex::bytes::RegexBuilder::new(&pattern)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|e| format!("Regex: {}", e))?;

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_clone = cancel.clone();
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        let mut batch = Vec::with_capacity(256);

        for m in re.find_iter(&data) {
            if cancel_clone.load(Ordering::Relaxed) {
                return;
            }

            batch.push(SearchHit {
                byte_offset: m.start(),
                match_len: Some(m.len()),
            });

            if batch.len() >= 256 {
                if tx.send(std::mem::take(&mut batch)).is_err() {
                    return;
                }
                batch = Vec::with_capacity(256);
            }
        }

        if !batch.is_empty() {
            let _ = tx.send(batch);
        }
    });

    Ok(SearchHandle {
        results: rx,
        cancel,
    })
}
