pub mod csv_loader;
pub mod excel_loader;

use std::collections::HashMap;
use std::path::Path;

/// Common trait for data sources
pub trait DataSource: Send {
    fn headers(&self) -> &[String];
    fn row_count(&self) -> usize;
    /// Get cell value at (row, col). Returns "" for out-of-bounds.
    fn cell(&self, row: usize, col: usize) -> &str;
    /// Raw bytes for search (only CSV/TSV has this)
    fn raw_bytes(&self) -> Option<&[u8]> {
        None
    }
    /// Get byte offset of a row (for search result mapping)
    #[allow(dead_code)]
    fn row_byte_offset(&self, row: usize) -> Option<usize> {
        let _ = row;
        None
    }
    /// Find which row contains a given byte offset
    fn row_for_byte_offset(&self, offset: usize) -> Option<usize> {
        let _ = offset;
        None
    }
    /// Number of sheets (1 for CSV)
    fn sheet_count(&self) -> usize {
        1
    }
    fn sheet_names(&self) -> Vec<String> {
        vec!["Sheet1".to_string()]
    }
    fn switch_sheet(&mut self, _index: usize) {}
    /// Detected encoding name
    fn encoding(&self) -> &str {
        "UTF-8"
    }
    /// Delimiter display name (e.g. "CSV (,)", "TSV (\\t)")
    fn delimiter_name(&self) -> &str {
        ""
    }
    /// File size in bytes
    fn file_size(&self) -> Option<u64> {
        None
    }
    /// Save edits back to the file (CSV only)
    fn save(&self, _path: &Path, _edits: &HashMap<(usize, usize), String>) -> Result<(), Box<dyn std::error::Error>> {
        Err("Save not supported for this file type".into())
    }
}
