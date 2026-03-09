use chardetng::EncodingDetector;
use memchr::memchr_iter;
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::DataSource;

const CHUNK_SIZE: usize = 64 * 1024 * 1024; // 64MB chunks for parallel scanning
const DETECT_SAMPLE_SIZE: usize = 64 * 1024; // 64KB sample for encoding detection

/// Backing data store: either mmap (zero-copy UTF-8) or decoded bytes
enum Backing {
    Mmap(Mmap),
    Decoded(Vec<u8>),
}

impl Backing {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Backing::Mmap(m) => m,
            Backing::Decoded(v) => v,
        }
    }
}

pub struct CsvLoader {
    backing: Backing,
    row_offsets: Vec<u64>,
    #[allow(dead_code)]
    data_start: usize,
    headers: Vec<String>,
    delimiter: u8,
    encoding_name: String,
    file_size: u64,
}

impl CsvLoader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(path.as_ref())?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let mmap = unsafe { Mmap::map(&file)? };

        // Detect encoding
        let (encoding, encoding_name) = detect_encoding(&mmap);

        // Build backing store
        let backing = if encoding == encoding_rs::UTF_8 {
            Backing::Mmap(mmap)
        } else {
            // Decode entire file to UTF-8
            let (decoded, _, _) = encoding.decode(&mmap);
            Backing::Decoded(decoded.into_owned().into_bytes())
        };

        let data = backing.as_bytes();
        let delimiter = detect_delimiter(data);
        let (headers, data_start) = parse_header(data, delimiter);
        let row_offsets = build_line_index_parallel(data, data_start);

        Ok(CsvLoader {
            backing,
            row_offsets,
            data_start,
            headers,
            delimiter,
            encoding_name,
            file_size,
        })
    }

    fn data(&self) -> &[u8] {
        self.backing.as_bytes()
    }

    /// Get the raw line bytes for a given row index
    fn row_bytes(&self, row: usize) -> &[u8] {
        if row >= self.row_offsets.len() {
            return b"";
        }
        let data = self.data();
        let start = self.row_offsets[row] as usize;
        let end = if row + 1 < self.row_offsets.len() {
            self.row_offsets[row + 1] as usize
        } else {
            data.len()
        };
        let mut slice = &data[start..end];
        if slice.last() == Some(&b'\n') {
            slice = &slice[..slice.len() - 1];
        }
        if slice.last() == Some(&b'\r') {
            slice = &slice[..slice.len() - 1];
        }
        slice
    }

    fn parse_row_fields(&self, row: usize) -> Vec<&str> {
        let line = self.row_bytes(row);
        parse_fields(line, self.delimiter)
    }

}

impl DataSource for CsvLoader {
    fn headers(&self) -> &[String] {
        &self.headers
    }

    fn row_count(&self) -> usize {
        self.row_offsets.len()
    }

    fn cell(&self, row: usize, col: usize) -> &str {
        if row >= self.row_offsets.len() {
            return "";
        }
        let fields = self.parse_row_fields(row);
        fields.get(col).copied().unwrap_or("")
    }


    fn raw_bytes(&self) -> Option<&[u8]> {
        Some(self.data())
    }

    fn row_byte_offset(&self, row: usize) -> Option<usize> {
        self.row_offsets.get(row).map(|&o| o as usize)
    }

    fn row_for_byte_offset(&self, offset: usize) -> Option<usize> {
        match self.row_offsets.binary_search(&(offset as u64)) {
            Ok(idx) => Some(idx),
            Err(idx) => {
                if idx == 0 {
                    None
                } else {
                    Some(idx - 1)
                }
            }
        }
    }

    fn encoding(&self) -> &str {
        &self.encoding_name
    }

    fn delimiter_name(&self) -> &str {
        match self.delimiter {
            b'\t' => "TSV (\\t)",
            b',' => "CSV (,)",
            b';' => "CSV (;)",
            b'|' => "CSV (|)",
            _ => "Delimited",
        }
    }

    fn file_size(&self) -> Option<u64> {
        Some(self.file_size)
    }

    fn save(&self, path: &Path, edits: &HashMap<(usize, usize), String>) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        let delim = self.delimiter;
        let delim_char = delim as char;

        // Write headers
        for (i, h) in self.headers.iter().enumerate() {
            if i > 0 {
                write!(writer, "{}", delim_char)?;
            }
            write_csv_field(&mut writer, h, delim)?;
        }
        writeln!(writer)?;

        // Write data rows
        let num_cols = self.headers.len();
        for row in 0..self.row_count() {
            let original_fields = self.parse_row_fields(row);
            for col in 0..num_cols {
                if col > 0 {
                    write!(writer, "{}", delim_char)?;
                }
                let val = if let Some(edited) = edits.get(&(row, col)) {
                    edited.as_str()
                } else {
                    original_fields.get(col).copied().unwrap_or("")
                };
                write_csv_field(&mut writer, val, delim)?;
            }
            writeln!(writer)?;
        }

        writer.flush()?;
        Ok(())
    }
}

/// Write a CSV field, quoting if necessary
fn write_csv_field<W: Write>(writer: &mut W, field: &str, delimiter: u8) -> std::io::Result<()> {
    let needs_quote = field.contains(delimiter as char)
        || field.contains('"')
        || field.contains('\n')
        || field.contains('\r');

    if needs_quote {
        write!(writer, "\"")?;
        for ch in field.chars() {
            if ch == '"' {
                write!(writer, "\"\"")?;
            } else {
                write!(writer, "{}", ch)?;
            }
        }
        write!(writer, "\"")?;
    } else {
        write!(writer, "{}", field)?;
    }
    Ok(())
}

/// Detect encoding using chardetng + heuristics
fn detect_encoding(data: &[u8]) -> (&'static encoding_rs::Encoding, String) {
    if data.is_empty() {
        return (encoding_rs::UTF_8, "UTF-8".to_string());
    }

    // Check BOM first
    if data.len() >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
        return (encoding_rs::UTF_8, "UTF-8 (BOM)".to_string());
    }
    if data.len() >= 2 && data[0] == 0xFF && data[1] == 0xFE {
        return (encoding_rs::UTF_16LE, "UTF-16LE".to_string());
    }
    if data.len() >= 2 && data[0] == 0xFE && data[1] == 0xFF {
        return (encoding_rs::UTF_16BE, "UTF-16BE".to_string());
    }

    // Use chardetng for auto-detection
    let sample_size = data.len().min(DETECT_SAMPLE_SIZE);
    let sample = &data[..sample_size];

    let mut detector = EncodingDetector::new();
    detector.feed(sample, sample_size >= data.len());
    let encoding = detector.guess(Some(b"ja"), true);
    let name = encoding.name().to_string();

    (encoding, name)
}

/// Detect delimiter by looking at the first line
fn detect_delimiter(data: &[u8]) -> u8 {
    let first_line_end = memchr::memchr(b'\n', data).unwrap_or(data.len().min(8192));
    let first_line = &data[..first_line_end];

    let tab_count = memchr::memchr_iter(b'\t', first_line).count();
    let comma_count = memchr::memchr_iter(b',', first_line).count();

    if tab_count > 0 && tab_count >= comma_count {
        b'\t'
    } else {
        b','
    }
}

/// Parse the header row
fn parse_header(data: &[u8], delimiter: u8) -> (Vec<String>, usize) {
    let line_end = memchr::memchr(b'\n', data).unwrap_or(data.len());
    let mut header_end = line_end;
    // Skip BOM if present
    let start = if data.len() >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
        3
    } else {
        0
    };
    let line = &data[start..header_end];
    let line = if line.last() == Some(&b'\r') {
        &line[..line.len() - 1]
    } else {
        line
    };

    let fields = parse_fields(line, delimiter);
    let headers: Vec<String> = fields.into_iter().map(|s| s.to_string()).collect();

    if header_end < data.len() {
        header_end += 1;
    }

    (headers, header_end)
}

/// Parse fields from a line, handling basic CSV quoting
fn parse_fields(line: &[u8], delimiter: u8) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut i = 0;
    let len = line.len();

    while i <= len {
        if i == len {
            fields.push("");
            break;
        }
        if line[i] == b'"' {
            let start = i + 1;
            i = start;
            loop {
                if i >= len {
                    break;
                }
                if line[i] == b'"' {
                    if i + 1 < len && line[i + 1] == b'"' {
                        i += 2;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            let end = i;
            let s = std::str::from_utf8(&line[start..end]).unwrap_or("");
            fields.push(s);
            if i < len {
                i += 1;
            }
            if i < len && line[i] == delimiter {
                i += 1;
            }
        } else {
            let start = i;
            match memchr::memchr(delimiter, &line[i..]) {
                Some(pos) => {
                    let end = i + pos;
                    let s = std::str::from_utf8(&line[start..end]).unwrap_or("");
                    fields.push(s);
                    i = end + 1;
                }
                None => {
                    let s = std::str::from_utf8(&line[start..len]).unwrap_or("");
                    fields.push(s);
                    break;
                }
            }
        }
    }

    fields
}

/// Build line index in parallel using rayon + memchr SIMD
fn build_line_index_parallel(data: &[u8], data_start: usize) -> Vec<u64> {
    let data_region = &data[data_start..];
    let total_len = data_region.len();

    if total_len == 0 {
        return vec![];
    }

    let num_chunks = total_len.div_ceil(CHUNK_SIZE);

    let chunk_results: Vec<Vec<u64>> = (0..num_chunks)
        .into_par_iter()
        .map(|chunk_idx| {
            let chunk_start = chunk_idx * CHUNK_SIZE;
            let chunk_end = (chunk_start + CHUNK_SIZE).min(total_len);
            let chunk = &data_region[chunk_start..chunk_end];

            let mut offsets = Vec::new();
            if chunk_idx == 0 {
                offsets.push(data_start as u64);
            }

            for pos in memchr_iter(b'\n', chunk) {
                let abs_pos = chunk_start + pos + 1;
                if abs_pos < total_len {
                    offsets.push((data_start + abs_pos) as u64);
                }
            }

            offsets
        })
        .collect();

    let mut row_offsets: Vec<u64> = Vec::with_capacity(
        chunk_results.iter().map(|v| v.len()).sum::<usize>() + 1,
    );
    for chunk in chunk_results {
        row_offsets.extend_from_slice(&chunk);
    }

    if let Some(&last) = row_offsets.last()
        && last as usize >= data.len() {
            row_offsets.pop();
    }

    row_offsets
}
