use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::mpsc;

use eframe::egui;

use crate::loader::csv_loader::CsvLoader;
use crate::loader::excel_loader::ExcelLoader;
use crate::loader::DataSource;
use crate::search::{self, SearchHandle, SearchHit};
use crate::viewer::{self, ViewerState};

/// Per-file tab state
pub struct FileTab {
    pub data: Option<Box<dyn DataSource>>,
    pub viewer_state: Option<ViewerState>,
    pub file_path: Option<PathBuf>,
    pub raw_data_cache: Option<Arc<Vec<u8>>>,
    /// Row byte offsets for synthetic search buffer (Excel files)
    pub synthetic_row_offsets: Option<Vec<u64>>,
    pub current_sheet: usize,
    // Search state (per-tab)
    pub search_visible: bool,
    pub search_query: String,
    pub search_handle: Option<SearchHandle>,
    pub search_results: Vec<SearchHit>,
    pub search_index: usize,
    pub search_done: bool,
    pub search_case_insensitive: bool,
    pub search_regex: bool,
    pub search_regex_error: Option<String>,
    pub replace_visible: bool,
    pub replace_text: String,
    pub goto_visible: bool,
    pub goto_text: String,
}

impl FileTab {
    fn new() -> Self {
        FileTab {
            data: None,
            viewer_state: None,
            file_path: None,
            raw_data_cache: None,
            synthetic_row_offsets: None,
            current_sheet: 0,
            search_visible: false,
            search_query: String::new(),
            search_handle: None,
            search_results: Vec::new(),
            search_index: 0,
            search_done: false,
            search_case_insensitive: false,
            search_regex: false,
            search_regex_error: None,
            replace_visible: false,
            replace_text: String::new(),
            goto_visible: false,
            goto_text: String::new(),
        }
    }

    fn tab_title(&self) -> String {
        if let Some(path) = &self.file_path
            && let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                let modified = self.viewer_state.as_ref().is_some_and(|s| s.modified);
                if modified {
                    return format!("*{}", name);
                }
                return name.to_string();
            }
        "New Tab".to_string()
    }

    fn open_file(&mut self, path: PathBuf) {
        self.close_file();

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let result: Result<Box<dyn DataSource>, Box<dyn std::error::Error>> = match ext.as_str() {
            "xlsx" | "xls" | "xlsb" | "xlsm" | "ods" => {
                ExcelLoader::open(&path).map(|l| Box::new(l) as Box<dyn DataSource>)
            }
            _ => {
                CsvLoader::open(&path).map(|l| Box::new(l) as Box<dyn DataSource>)
            }
        };

        match result {
            Ok(data) => {
                let num_cols = data.headers().len();
                self.raw_data_cache = data.raw_bytes().map(|b| Arc::new(b.to_vec()));
                self.synthetic_row_offsets = None;
                self.viewer_state = Some(ViewerState::new(num_cols));
                self.data = Some(data);
                self.file_path = Some(path);
                self.build_synthetic_search_buffer();
            }
            Err(_e) => {
                // Error will be set by caller
            }
        }
    }

    fn close_file(&mut self) {
        self.cancel_search();
        self.data = None;
        self.viewer_state = None;
        self.file_path = None;
        self.raw_data_cache = None;
        self.synthetic_row_offsets = None;
        self.search_results.clear();
        self.search_visible = false;
        self.current_sheet = 0;
    }

    /// Build a synthetic search buffer for data sources without raw bytes (Excel)
    fn build_synthetic_search_buffer(&mut self) {
        if self.raw_data_cache.is_some() {
            return; // Already has native raw bytes (CSV)
        }
        let Some(data) = &self.data else { return };
        let num_cols = data.headers().len();
        let row_count = data.row_count();

        let mut buf = Vec::with_capacity(row_count * num_cols * 10);
        let mut row_offsets = Vec::with_capacity(row_count);

        for row in 0..row_count {
            row_offsets.push(buf.len() as u64);
            for col in 0..num_cols {
                if col > 0 {
                    buf.push(b'\t');
                }
                buf.extend_from_slice(data.cell(row, col).as_bytes());
            }
            buf.push(b'\n');
        }

        self.raw_data_cache = Some(Arc::new(buf));
        self.synthetic_row_offsets = Some(row_offsets);
    }

    fn cancel_search(&mut self) {
        if let Some(handle) = self.search_handle.take() {
            handle.cancel.store(true, Ordering::Relaxed);
        }
    }

    fn start_search(&mut self) {
        self.cancel_search();
        self.search_results.clear();
        self.search_index = 0;
        self.search_done = false;
        self.search_regex_error = None;

        if self.search_query.is_empty() {
            return;
        }

        let Some(raw) = &self.raw_data_cache else {
            // No raw bytes available (should not happen after synthetic buffer init)
            self.search_done = true;
            return;
        };

        if self.search_regex {
            match search::search_background_regex(
                raw.clone(),
                self.search_query.clone(),
                self.search_case_insensitive,
            ) {
                Ok(handle) => self.search_handle = Some(handle),
                Err(e) => self.search_regex_error = Some(e),
            }
        } else {
            let handle = if self.search_case_insensitive {
                search::search_background_case_insensitive(
                    raw.clone(),
                    self.search_query.clone(),
                )
            } else {
                search::search_background(
                    raw.clone(),
                    self.search_query.clone(),
                )
            };
            self.search_handle = Some(handle);
        }
    }

    fn poll_search_results(&mut self) {
        if let Some(handle) = &self.search_handle {
            loop {
                match handle.results.try_recv() {
                    Ok(batch) => {
                        self.search_results.extend(batch);
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        self.search_done = true;
                        break;
                    }
                }
            }
        }
    }

    fn navigate_search(&mut self, forward: bool) {
        if self.search_results.is_empty() {
            return;
        }
        if forward {
            self.search_index = (self.search_index + 1) % self.search_results.len();
        } else {
            self.search_index = if self.search_index == 0 {
                self.search_results.len() - 1
            } else {
                self.search_index - 1
            };
        }
        self.jump_to_search_result();
    }

    fn jump_to_search_result(&mut self) {
        let byte_offset = self.search_results.get(self.search_index).map(|h| h.byte_offset);
        let Some(offset) = byte_offset else { return };
        if let Some(actual_row) = self.resolve_row(offset)
            && let Some(state) = &mut self.viewer_state {
                let display_row = state
                    .filtered_indices
                    .iter()
                    .position(|&idx| idx as usize == actual_row)
                    .unwrap_or(0);
                state.scroll_to_row = Some(display_row);
                state.highlight_row = Some(display_row);
            }
    }

    /// Resolve actual row from a search hit byte offset
    fn resolve_row(&self, byte_offset: usize) -> Option<usize> {
        if let Some(data) = &self.data
            && let Some(row) = data.row_for_byte_offset(byte_offset) {
                return Some(row);
            }
        // Fallback: synthetic_row_offsets (Excel)
        if let Some(offsets) = &self.synthetic_row_offsets {
            let offset = byte_offset as u64;
            return match offsets.binary_search(&offset) {
                Ok(idx) => Some(idx),
                Err(idx) => if idx == 0 { None } else { Some(idx - 1) },
            };
        }
        None
    }

    /// Find column + match text for a hit, with synthetic buffer fallback
    fn resolve_col_for_hit(&self, actual_row: usize, hit: &SearchHit, query: &str) -> Option<(usize, String)> {
        if let Some(data) = &self.data {
            // Try native (CSV) first
            if let Some(result) = find_col_for_hit(data.as_ref(), actual_row, hit, query) {
                return Some(result);
            }
        }
        // Fallback: synthetic buffer (Excel)
        let raw = self.raw_data_cache.as_ref()?;
        let offsets = self.synthetic_row_offsets.as_ref()?;
        let row_start = *offsets.get(actual_row)? as usize;
        let row_end = offsets.get(actual_row + 1).map(|&o| o as usize).unwrap_or(raw.len());
        let row_bytes = &raw[row_start..row_end];
        let offset_in_row = hit.byte_offset.checked_sub(row_start)?;

        // Synthetic buffer uses tab delimiter
        let mut col = 0usize;
        let mut pos = 0usize;
        let effective_len = row_bytes.len().saturating_sub(1); // strip trailing \n

        while pos < effective_len {
            let field_start = pos;
            match memchr::memchr(b'\t', &row_bytes[pos..effective_len]) {
                Some(dp) => { pos += dp + 1; }
                None => { pos = effective_len; }
            }
            if offset_in_row >= field_start && offset_in_row < pos {
                let match_len = hit.match_len.unwrap_or(query.len());
                let match_end = (hit.byte_offset + match_len).min(raw.len());
                let match_text = String::from_utf8_lossy(&raw[hit.byte_offset..match_end]).to_string();
                return Some((col, match_text));
            }
            col += 1;
        }
        None
    }

    fn replace_current(&mut self) {
        if self.search_results.is_empty() {
            return;
        }
        let hit = self.search_results[self.search_index].clone();
        let query = self.search_query.clone();
        let replace = self.replace_text.clone();
        let is_regex = self.search_regex;
        let case_insensitive = self.search_case_insensitive;

        if let Some(actual_row) = self.resolve_row(hit.byte_offset)
            && let Some((col, match_text)) = self.resolve_col_for_hit(actual_row, &hit, &query) {
                let replacement = compute_replacement(&query, &replace, &match_text, is_regex, case_insensitive);
                if let (Some(data), Some(state)) = (&self.data, &mut self.viewer_state) {
                    let current_val = state.get_cell(data.as_ref(), actual_row, col).to_string();
                    let new_val = current_val.replacen(&match_text, &replacement, 1);
                    state.commit_edit(actual_row, col, new_val);
                }
                self.search_results.remove(self.search_index);
                if !self.search_results.is_empty() {
                    self.search_index = self.search_index.min(self.search_results.len() - 1);
                    self.jump_to_search_result();
                }
            }
    }

    fn replace_all(&mut self) {
        if self.search_results.is_empty() {
            return;
        }
        let query = self.search_query.clone();
        let replace = self.replace_text.clone();
        let is_regex = self.search_regex;
        let case_insensitive = self.search_case_insensitive;

        let mut cell_replacements: std::collections::HashMap<(usize, usize), Vec<String>> = std::collections::HashMap::new();
        for hit in &self.search_results {
            if let Some(actual_row) = self.resolve_row(hit.byte_offset)
                && let Some((col, match_text)) = self.resolve_col_for_hit(actual_row, hit, &query) {
                    cell_replacements.entry((actual_row, col)).or_default().push(match_text);
                }
        }

        if let (Some(data), Some(state)) = (&self.data, &mut self.viewer_state) {
            for ((row, col), matches) in cell_replacements {
                let mut val = state.get_cell(data.as_ref(), row, col).to_string();
                for m in &matches {
                    let replacement = compute_replacement(&query, &replace, m, is_regex, case_insensitive);
                    val = val.replacen(m, &replacement, 1);
                }
                state.commit_edit(row, col, val);
            }
        }
        self.search_results.clear();
        self.search_index = 0;
    }

    fn save_file(&mut self) -> Result<(), String> {
        let Some(path) = &self.file_path else { return Ok(()); };
        let Some(data) = &self.data else { return Ok(()); };
        let Some(state) = &self.viewer_state else { return Ok(()); };

        if !state.modified {
            return Ok(());
        }

        match data.save(path, &state.edits) {
            Ok(()) => {
                let path = path.clone();
                self.open_file(path);
                Ok(())
            }
            Err(e) => Err(format!("Save failed: {}", e)),
        }
    }
}

/// Row export mode for the export dialog
#[derive(Clone, Copy, PartialEq)]
pub enum RowExportMode {
    AllFiltered,
    SelectionOnly,
    RowRange,
}

/// Export settings dialog state
pub struct ExportSettings {
    pub show: bool,
    pub col_selected: Vec<bool>,
    pub col_names: Vec<String>,
    pub row_mode: RowExportMode,
    pub range_from: String,
    pub range_to: String,
    pub row_limit: String,
    pub sampling_every: String,
    pub total_filtered: usize,
    pub has_selection: bool,
    pub selection_rows: usize,
}

impl Default for ExportSettings {
    fn default() -> Self {
        ExportSettings {
            show: false,
            col_selected: Vec::new(),
            col_names: Vec::new(),
            row_mode: RowExportMode::AllFiltered,
            range_from: "1".to_string(),
            range_to: String::new(),
            row_limit: String::new(),
            sampling_every: "1".to_string(),
            total_filtered: 0,
            has_selection: false,
            selection_rows: 0,
        }
    }
}

pub struct App {
    tabs: Vec<FileTab>,
    active_tab: usize,
    error_msg: Option<String>,
    show_about: bool,
    export_settings: ExportSettings,
    notify_rx: mpsc::Receiver<String>,
    notify_tx: mpsc::Sender<String>,
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>, initial_file: Option<PathBuf>) -> Self {
        configure_fonts(&cc.egui_ctx);

        let (notify_tx, notify_rx) = mpsc::channel();
        let mut app = App {
            tabs: Vec::new(),
            active_tab: 0,
            error_msg: None,
            show_about: false,
            export_settings: ExportSettings::default(),
            notify_rx,
            notify_tx,
        };

        if let Some(path) = initial_file {
            app.open_file_in_new_tab(path);
        }

        app
    }

    fn active_tab(&self) -> Option<&FileTab> {
        self.tabs.get(self.active_tab)
    }

    fn active_tab_mut(&mut self) -> Option<&mut FileTab> {
        self.tabs.get_mut(self.active_tab)
    }

    fn open_file_in_new_tab(&mut self, path: PathBuf) {
        let mut tab = FileTab::new();
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let result: Result<Box<dyn DataSource>, Box<dyn std::error::Error>> = match ext.as_str() {
            "xlsx" | "xls" | "xlsb" | "xlsm" | "ods" => {
                ExcelLoader::open(&path).map(|l| Box::new(l) as Box<dyn DataSource>)
            }
            _ => {
                CsvLoader::open(&path).map(|l| Box::new(l) as Box<dyn DataSource>)
            }
        };

        match result {
            Ok(data) => {
                let num_cols = data.headers().len();
                tab.raw_data_cache = data.raw_bytes().map(|b| Arc::new(b.to_vec()));
                tab.viewer_state = Some(ViewerState::new(num_cols));
                tab.data = Some(data);
                tab.file_path = Some(path);
                tab.build_synthetic_search_buffer();
                self.error_msg = None;
                self.tabs.push(tab);
                self.active_tab = self.tabs.len() - 1;
            }
            Err(e) => {
                self.error_msg = Some(format!("Failed to open file: {}", e));
            }
        }
    }

    fn close_tab(&mut self, index: usize) {
        if index < self.tabs.len() {
            self.tabs[index].cancel_search();
            self.tabs.remove(index);
            if self.active_tab >= self.tabs.len() && !self.tabs.is_empty() {
                self.active_tab = self.tabs.len() - 1;
            }
        }
    }

    fn open_file_dialog(&mut self) {
        if let Some(path) = rfd::FileDialog::new()
            .add_filter("Data Files", &["csv", "tsv", "txt", "xlsx", "xls", "xlsb", "ods"])
            .add_filter("All Files", &["*"])
            .pick_file()
        {
            self.open_file_in_new_tab(path);
        }
    }

    fn open_export_dialog(&mut self) {
        let Some(tab) = self.active_tab() else { return };
        let Some(data) = &tab.data else { return };
        let Some(state) = &tab.viewer_state else { return };

        let headers = data.headers();
        let num_actual = headers.len();

        let mut col_selected = vec![false; num_actual];
        for &ac in &state.visible_cols {
            if ac < num_actual {
                col_selected[ac] = true;
            }
        }

        let has_selection = state.selection_range().is_some();
        let selection_rows = state.selection_range()
            .map(|(tl, br)| br.row - tl.row + 1)
            .unwrap_or(0);

        self.export_settings = ExportSettings {
            show: true,
            col_selected,
            col_names: headers.to_vec(),
            row_mode: RowExportMode::AllFiltered,
            range_from: "1".to_string(),
            range_to: state.filtered_indices.len().to_string(),
            row_limit: String::new(),
            sampling_every: "1".to_string(),
            total_filtered: state.filtered_indices.len(),
            has_selection,
            selection_rows,
        };
    }

    fn do_export_with_settings(&mut self) {
        let Some(tab) = self.active_tab() else { return };
        let Some(data) = &tab.data else { return };
        let Some(state) = &tab.viewer_state else { return };

        let Some(path) = rfd::FileDialog::new()
            .add_filter("CSV", &["csv"])
            .add_filter("TSV", &["tsv"])
            .add_filter("Excel", &["xlsx"])
            .set_file_name("export.csv")
            .save_file()
        else {
            return;
        };

        let result = export_with_settings(&path, data.as_ref(), state, &self.export_settings);
        if let Err(e) = result {
            self.error_msg = Some(format!("Export failed: {}", e));
        }
        self.export_settings.show = false;
    }

    fn window_title(&self) -> String {
        let mut title = String::from("GridPeek");
        if let Some(tab) = self.active_tab() {
            if let Some(path) = &tab.file_path
                && let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    title = format!("{} - GridPeek", name);
                }
            if let Some(state) = &tab.viewer_state
                && state.modified {
                    title = format!("[Modified] {}", title);
                }
        }
        title
    }
}

fn write_export_field<W: Write>(writer: &mut W, field: &str, delimiter: u8) -> std::io::Result<()> {
    let bytes = field.as_bytes();
    let needs_quote = memchr::memchr3(delimiter, b'"', b'\n', bytes).is_some()
        || memchr::memchr(b'\r', bytes).is_some();

    if needs_quote {
        writer.write_all(b"\"")?;
        let mut start = 0;
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'"' {
                writer.write_all(&bytes[start..i])?;
                writer.write_all(b"\"\"")?;
                start = i + 1;
            }
        }
        writer.write_all(&bytes[start..])?;
        writer.write_all(b"\"")?;
    } else {
        writer.write_all(bytes)?;
    }
    Ok(())
}

/// Collect export row indices based on settings
fn collect_export_rows(state: &ViewerState, settings: &ExportSettings) -> Vec<usize> {
    let sampling = settings.sampling_every.parse::<usize>().unwrap_or(1).max(1);
    let limit = settings.row_limit.parse::<usize>().unwrap_or(usize::MAX);

    let slice: &[u32] = match settings.row_mode {
        RowExportMode::AllFiltered => &state.filtered_indices,
        RowExportMode::SelectionOnly => {
            if let Some((tl, br)) = state.selection_range() {
                let end = (br.row + 1).min(state.filtered_indices.len());
                &state.filtered_indices[tl.row..end]
            } else {
                &[]
            }
        }
        RowExportMode::RowRange => {
            let from = settings.range_from.parse::<usize>().unwrap_or(1).max(1) - 1;
            let to = settings.range_to.parse::<usize>().unwrap_or(state.filtered_indices.len());
            let end = to.min(state.filtered_indices.len());
            let start = from.min(end);
            &state.filtered_indices[start..end]
        }
    };

    slice.iter()
        .step_by(sampling)
        .take(limit)
        .map(|&r| r as usize)
        .collect()
}

/// Export with settings (CSV/TSV/XLSX)
fn export_with_settings(
    path: &std::path::Path,
    data: &dyn DataSource,
    state: &ViewerState,
    settings: &ExportSettings,
) -> Result<(), Box<dyn std::error::Error>> {
    // Collect selected columns
    let export_cols: Vec<usize> = settings.col_selected.iter()
        .enumerate()
        .filter(|&(_, selected)| *selected)
        .map(|(i, _)| i)
        .collect();

    if export_cols.is_empty() {
        return Err("No columns selected".into());
    }

    // Collect rows
    let export_rows = collect_export_rows(state, settings);

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    if ext == "xlsx" {
        return export_xlsx_with_settings(path, data, state, &export_cols, &export_rows);
    }

    let delim: u8 = if ext == "tsv" { b'\t' } else { b',' };

    let file = std::fs::File::create(path)?;
    let mut writer = BufWriter::new(file);

    let headers = data.headers();

    let delim_byte = [delim];

    // Write headers
    for (i, &ac) in export_cols.iter().enumerate() {
        if i > 0 {
            writer.write_all(&delim_byte)?;
        }
        let h = headers.get(ac).map_or("", |s| s.as_str());
        write_export_field(&mut writer, h, delim)?;
    }
    writer.write_all(b"\n")?;

    // Write data rows
    for &actual in &export_rows {
        for (i, &ac) in export_cols.iter().enumerate() {
            if i > 0 {
                writer.write_all(&delim_byte)?;
            }
            let val = state.get_cell(data, actual, ac);
            write_export_field(&mut writer, val, delim)?;
        }
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    Ok(())
}

/// Export to xlsx with settings
fn export_xlsx_with_settings(
    path: &std::path::Path,
    data: &dyn DataSource,
    state: &ViewerState,
    export_cols: &[usize],
    export_rows: &[usize],
) -> Result<(), Box<dyn std::error::Error>> {
    use rust_xlsxwriter::{Format, Workbook};

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();

    let bold = Format::new().set_bold();
    let headers = data.headers();

    // Write headers
    for (i, &ac) in export_cols.iter().enumerate() {
        let h = headers.get(ac).map_or("", |s| s.as_str());
        worksheet.write_string_with_format(0, i as u16, h, &bold)?;
    }

    // Write data rows
    for (row_out, &actual) in export_rows.iter().enumerate() {
        for (col_out, &ac) in export_cols.iter().enumerate() {
            let val = state.get_cell(data, actual, ac);
            if let Ok(n) = val.parse::<f64>() {
                worksheet.write_number((row_out + 1) as u32, col_out as u16, n)?;
            } else {
                worksheet.write_string((row_out + 1) as u32, col_out as u16, val)?;
            }
        }
    }

    workbook.save(path)?;
    Ok(())
}

/// Find which column a byte-offset hit falls into, and extract the matched text.
fn find_col_for_hit(data: &dyn DataSource, actual_row: usize, hit: &SearchHit, query: &str) -> Option<(usize, String)> {
    let row_start = data.row_byte_offset(actual_row)?;
    let offset_in_row = hit.byte_offset.checked_sub(row_start)?;
    let raw = data.raw_bytes()?;

    let row_end = data.row_byte_offset(actual_row + 1).unwrap_or(raw.len());
    let row_bytes = &raw[row_start..row_end];

    let tab_count = memchr::memchr_iter(b'\t', row_bytes).count();
    let comma_count = memchr::memchr_iter(b',', row_bytes).count();
    let delimiter = if tab_count > 0 && tab_count >= comma_count { b'\t' } else { b',' };

    let mut effective_len = row_bytes.len();
    if effective_len > 0 && row_bytes[effective_len - 1] == b'\n' { effective_len -= 1; }
    if effective_len > 0 && row_bytes[effective_len - 1] == b'\r' { effective_len -= 1; }

    let mut col = 0usize;
    let mut pos = 0usize;

    while pos < effective_len {
        let field_start = pos;
        if row_bytes[pos] == b'"' {
            pos += 1;
            loop {
                if pos >= effective_len { break; }
                if row_bytes[pos] == b'"' {
                    if pos + 1 < effective_len && row_bytes[pos + 1] == b'"' { pos += 2; }
                    else { pos += 1; break; }
                } else { pos += 1; }
            }
            if pos < effective_len && row_bytes[pos] == delimiter { pos += 1; }
        } else {
            match memchr::memchr(delimiter, &row_bytes[pos..effective_len]) {
                Some(dp) => { pos += dp + 1; }
                None => { pos = effective_len; }
            }
        }

        let next_field_start = pos;
        if offset_in_row >= field_start && offset_in_row < next_field_start {
            let match_len = hit.match_len.unwrap_or(query.len());
            let match_end = (hit.byte_offset + match_len).min(raw.len());
            let match_bytes = &raw[hit.byte_offset..match_end];
            let match_text = String::from_utf8_lossy(match_bytes).to_string();
            return Some((col, match_text));
        }
        col += 1;
    }

    if offset_in_row >= pos {
        let match_len = hit.match_len.unwrap_or(query.len());
        let match_end = (hit.byte_offset + match_len).min(raw.len());
        let match_bytes = &raw[hit.byte_offset..match_end];
        let match_text = String::from_utf8_lossy(match_bytes).to_string();
        return Some((col, match_text));
    }

    None
}

/// Compute the replacement string, handling regex capture groups if needed.
fn compute_replacement(query: &str, replace: &str, match_text: &str, is_regex: bool, case_insensitive: bool) -> String {
    if is_regex
        && let Ok(re) = regex::RegexBuilder::new(query)
            .case_insensitive(case_insensitive)
            .build()
            && let Some(caps) = re.captures(match_text) {
                let mut result = replace.to_string();
                for i in (0..caps.len()).rev() {
                    if let Some(m) = caps.get(i) {
                        result = result.replace(&format!("${}", i), m.as_str());
                    }
                }
                return result;
            }
    replace.to_string()
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.send_viewport_cmd(egui::ViewportCommand::Title(self.window_title()));

        // Poll notifications from background threads
        while let Ok(msg) = self.notify_rx.try_recv() {
            self.error_msg = Some(msg);
        }

        // Poll search for active tab
        if let Some(tab) = self.active_tab_mut() {
            tab.poll_search_results();
        }

        let dialog_open = self.export_settings.show || self.show_about;

        let mut open_file = false;
        let mut toggle_search = false;
        let mut toggle_replace = false;
        let mut copy = false;
        let mut toggle_filter = false;
        let mut search_next = false;
        let mut search_prev = false;
        let mut save = false;
        let mut undo = false;
        let mut goto = false;
        let mut auto_width = false;
        let mut zoom_in = false;
        let mut zoom_out = false;
        let mut zoom_reset = false;
        let mut close_tab = false;
        let mut next_tab = false;
        let mut prev_tab = false;

        let mut nav_dr: i32 = 0;
        let mut nav_dc: i32 = 0;
        let mut nav_extend = false;
        let mut nav_home = false;
        let mut nav_end = false;
        let mut nav_ctrl_home = false;
        let mut nav_ctrl_end = false;
        let mut nav_page_up = false;
        let mut nav_page_down = false;

        let tab_search_visible = self.active_tab().is_some_and(|t| t.search_visible);
        let tab_replace_visible = self.active_tab().is_some_and(|t| t.replace_visible);
        let tab_goto_visible = self.active_tab().is_some_and(|t| t.goto_visible);
        let tab_editing = self.active_tab().is_some_and(|t| {
            t.viewer_state.as_ref().is_some_and(|s| s.editing_cell.is_some())
        });
        let tab_show_filters = self.active_tab().is_some_and(|t| {
            t.viewer_state.as_ref().is_some_and(|s| s.show_filters)
        });

        let has_text_focus = ctx.memory(|m| m.focused().is_some())
            && (tab_search_visible || tab_replace_visible || tab_goto_visible
                || tab_editing || tab_show_filters);

        ctx.input(|i| {
            if dialog_open {
                // Only Escape works while a dialog is open
                if i.key_pressed(egui::Key::Escape) {
                    if self.export_settings.show {
                        self.export_settings.show = false;
                    } else if self.show_about {
                        self.show_about = false;
                    }
                }
                return;
            }
            if i.key_pressed(egui::Key::O) && i.modifiers.command { open_file = true; }
            if i.key_pressed(egui::Key::F) && i.modifiers.command { toggle_search = true; }
            if i.key_pressed(egui::Key::H) && i.modifiers.command { toggle_replace = true; }
            if i.key_pressed(egui::Key::C) && i.modifiers.command { copy = true; }
            if i.key_pressed(egui::Key::L) && i.modifiers.command { toggle_filter = true; }
            if i.key_pressed(egui::Key::S) && i.modifiers.command { save = true; }
            if i.key_pressed(egui::Key::Z) && i.modifiers.command { undo = true; }
            if i.key_pressed(egui::Key::G) && i.modifiers.command { goto = true; }
            // Ctrl+W = close tab, Ctrl+Shift+W = auto width
            if i.key_pressed(egui::Key::W) && i.modifiers.command {
                if i.modifiers.shift {
                    auto_width = true;
                } else {
                    close_tab = true;
                }
            }
            if i.key_pressed(egui::Key::Equals) && i.modifiers.command { zoom_in = true; }
            if i.key_pressed(egui::Key::Minus) && i.modifiers.command { zoom_out = true; }
            if i.key_pressed(egui::Key::Num0) && i.modifiers.command { zoom_reset = true; }
            if i.modifiers.command && i.raw_scroll_delta.y != 0.0 {
                if i.raw_scroll_delta.y > 0.0 { zoom_in = true; }
                else { zoom_out = true; }
            }
            // Tab switching: Ctrl+Tab / Ctrl+Shift+Tab
            if i.key_pressed(egui::Key::Tab) && i.modifiers.command {
                if i.modifiers.shift { prev_tab = true; } else { next_tab = true; }
            }
            if i.key_pressed(egui::Key::Escape)
                && let Some(tab) = self.tabs.get_mut(self.active_tab) {
                    if tab.replace_visible {
                        tab.replace_visible = false;
                    } else if tab.search_visible {
                        tab.search_visible = false;
                        tab.cancel_search();
                        if let Some(state) = &mut tab.viewer_state {
                            state.highlight_row = None;
                        }
                    }
                    if tab.goto_visible {
                        tab.goto_visible = false;
                    }
                }
            if i.key_pressed(egui::Key::Enter) && tab_search_visible {
                if i.modifiers.shift { search_prev = true; } else { search_next = true; }
            }

            if !has_text_focus {
                if i.key_pressed(egui::Key::ArrowUp) { nav_dr = -1; nav_extend = i.modifiers.shift; }
                if i.key_pressed(egui::Key::ArrowDown) { nav_dr = 1; nav_extend = i.modifiers.shift; }
                if i.key_pressed(egui::Key::ArrowLeft) { nav_dc = -1; nav_extend = i.modifiers.shift; }
                if i.key_pressed(egui::Key::ArrowRight) { nav_dc = 1; nav_extend = i.modifiers.shift; }
                if i.key_pressed(egui::Key::Home) {
                    if i.modifiers.command { nav_ctrl_home = true; } else { nav_home = true; }
                    nav_extend = i.modifiers.shift;
                }
                if i.key_pressed(egui::Key::End) {
                    if i.modifiers.command { nav_ctrl_end = true; } else { nav_end = true; }
                    nav_extend = i.modifiers.shift;
                }
                if i.key_pressed(egui::Key::PageUp) { nav_page_up = true; nav_extend = i.modifiers.shift; }
                if i.key_pressed(egui::Key::PageDown) { nav_page_down = true; nav_extend = i.modifiers.shift; }
            }
        });

        // Handle dropped files - open each in a new tab
        if !dialog_open {
            let dropped_files: Vec<PathBuf> = ctx.input(|i| {
                i.raw.dropped_files.iter().filter_map(|f| f.path.clone()).collect()
            });
            for path in dropped_files {
                self.open_file_in_new_tab(path);
            }
        }

        if open_file { self.open_file_dialog(); }

        // Tab switching
        if next_tab && !self.tabs.is_empty() {
            self.active_tab = (self.active_tab + 1) % self.tabs.len();
        }
        if prev_tab && !self.tabs.is_empty() {
            self.active_tab = if self.active_tab == 0 { self.tabs.len() - 1 } else { self.active_tab - 1 };
        }
        if close_tab
            && !self.tabs.is_empty() {
                let idx = self.active_tab;
                self.close_tab(idx);
            }

        // Save needs special handling because tab.save_file() error goes to self.error_msg
        let mut save_error: Option<String> = None;
        if let Some(tab) = self.active_tab_mut() {
            if toggle_search {
                tab.search_visible = !tab.search_visible;
                if !tab.search_visible {
                    tab.cancel_search();
                    tab.replace_visible = false;
                    if let Some(state) = &mut tab.viewer_state { state.highlight_row = None; }
                }
            }
            if toggle_replace {
                tab.search_visible = true;
                tab.replace_visible = !tab.replace_visible;
            }
            if toggle_filter
                && let Some(state) = &mut tab.viewer_state { state.show_filters = !state.show_filters; }
            if copy
                && let (Some(data), Some(state)) = (&tab.data, &tab.viewer_state) {
                    let text = state.copy_selection(data.as_ref());
                    if !text.is_empty()
                        && let Ok(mut clipboard) = arboard::Clipboard::new() { let _ = clipboard.set_text(text); }
                }
            if save
                && let Err(e) = tab.save_file() {
                    save_error = Some(e);
                }
            if undo && let Some(state) = &mut tab.viewer_state { state.undo(); }
            if goto { tab.goto_visible = !tab.goto_visible; tab.goto_text.clear(); }
            if auto_width
                && let (Some(data), Some(state)) = (&tab.data, &mut tab.viewer_state) {
                    let num_cols = data.headers().len();
                    state.compute_auto_widths(data.as_ref(), num_cols);
                }
            if search_next { tab.navigate_search(true); }
            if search_prev { tab.navigate_search(false); }
        }
        if let Some(e) = save_error {
            self.error_msg = Some(e);
        }

        // Zoom
        if zoom_in || zoom_out || zoom_reset {
            let current = ctx.pixels_per_point();
            let new_ppp = if zoom_reset {
                1.0
            } else if zoom_in {
                (current + 0.1).min(3.0)
            } else {
                (current - 0.1).max(0.5)
            };
            ctx.set_pixels_per_point(new_ppp);
        }

        // Apply keyboard navigation
        if let Some(tab) = self.active_tab_mut()
            && let Some(state) = &mut tab.viewer_state {
                let num_cols = tab.data.as_ref().map_or(0, |d| d.headers().len());
                if nav_dr != 0 || nav_dc != 0 { state.move_cursor(nav_dr, nav_dc, nav_extend, num_cols); }
                if nav_home { let r = state.cursor.map_or(0, |c| c.row); state.jump_cursor(r, 0, nav_extend, num_cols); }
                if nav_end {
                    let r = state.cursor.map_or(0, |c| c.row);
                    let last_col = state.visible_cols.len().saturating_sub(1);
                    state.jump_cursor(r, last_col, nav_extend, num_cols);
                }
                if nav_ctrl_home { state.jump_cursor(0, 0, nav_extend, num_cols); }
                if nav_ctrl_end {
                    let lr = state.display_row_count().saturating_sub(1);
                    let last_col = state.visible_cols.len().saturating_sub(1);
                    state.jump_cursor(lr, last_col, nav_extend, num_cols);
                }
                if nav_page_up { state.move_cursor(-30, 0, nav_extend, num_cols); }
                if nav_page_down { state.move_cursor(30, 0, nav_extend, num_cols); }

                if (nav_dr != 0 || nav_dc != 0 || nav_home || nav_end || nav_ctrl_home || nav_ctrl_end || nav_page_up || nav_page_down)
                    && let Some(data) = &tab.data { state.update_selection_stats(data.as_ref()); }
            }

        // Top panel
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui.button("Open... (Ctrl+O)").clicked() { ui.close_menu(); self.open_file_dialog(); }
                    if self.active_tab().is_some_and(|t| t.data.is_some()) {
                        if ui.button("Save (Ctrl+S)").clicked() {
                            ui.close_menu();
                            if let Some(tab) = self.active_tab_mut()
                                && let Err(e) = tab.save_file() {
                                    self.error_msg = Some(e);
                                }
                        }
                        if ui.button("Export View...").clicked() { ui.close_menu(); self.open_export_dialog(); }
                    }
                    ui.separator();
                    if !self.tabs.is_empty()
                        && ui.button("Close Tab (Ctrl+W)").clicked() {
                            ui.close_menu();
                            let idx = self.active_tab;
                            self.close_tab(idx);
                        }
                });
                ui.menu_button("Edit", |ui| {
                    if ui.button("Undo (Ctrl+Z)").clicked() {
                        ui.close_menu();
                        if let Some(tab) = self.active_tab_mut()
                            && let Some(state) = &mut tab.viewer_state { state.undo(); }
                    }
                    ui.separator();
                    if ui.button("Copy (Ctrl+C)").clicked() {
                        ui.close_menu();
                        if let Some(tab) = self.active_tab_mut()
                            && let (Some(data), Some(state)) = (&tab.data, &tab.viewer_state) {
                                let text = state.copy_selection(data.as_ref());
                                if !text.is_empty()
                                    && let Ok(mut clipboard) = arboard::Clipboard::new() { let _ = clipboard.set_text(text); }
                            }
                    }
                    if ui.button("Find (Ctrl+F)").clicked() {
                        ui.close_menu();
                        if let Some(tab) = self.active_tab_mut() { tab.search_visible = true; }
                    }
                    if ui.button("Find & Replace (Ctrl+H)").clicked() {
                        ui.close_menu();
                        if let Some(tab) = self.active_tab_mut() {
                            tab.search_visible = true;
                            tab.replace_visible = true;
                        }
                    }
                    if ui.button("Go to Row... (Ctrl+G)").clicked() {
                        ui.close_menu();
                        if let Some(tab) = self.active_tab_mut() {
                            tab.goto_visible = true;
                            tab.goto_text.clear();
                        }
                    }
                    if ui.button("Toggle Filters (Ctrl+L)").clicked() {
                        ui.close_menu();
                        if let Some(tab) = self.active_tab_mut()
                            && let Some(state) = &mut tab.viewer_state { state.show_filters = !state.show_filters; }
                    }
                    ui.separator();
                    {
                        let dup_on = self.active_tab().is_some_and(|t| {
                            t.viewer_state.as_ref().is_some_and(|s| s.highlight_duplicates)
                        });
                        let label = if dup_on { "Disable Highlight Duplicates" } else { "Highlight Duplicates" };
                        if ui.button(label).clicked() {
                            ui.close_menu();
                            if let Some(tab) = self.active_tab_mut()
                                && let (Some(data), Some(state)) = (&tab.data, &mut tab.viewer_state) {
                                    state.highlight_duplicates = !state.highlight_duplicates;
                                    if state.highlight_duplicates {
                                        state.compute_duplicate_rows(data.as_ref());
                                    } else {
                                        state.duplicate_rows.clear();
                                        state.duplicate_count = 0;
                                    }
                                }
                        }
                    }
                    ui.separator();
                    if ui.button("Conditional Highlight...").clicked() {
                        ui.close_menu();
                        if let Some(tab) = self.active_tab_mut()
                            && let Some(state) = &mut tab.viewer_state {
                                state.show_highlight_dialog = true;
                            }
                    }
                    if ui.button("Clear All Highlights").clicked() {
                        ui.close_menu();
                        if let Some(tab) = self.active_tab_mut()
                            && let Some(state) = &mut tab.viewer_state {
                                state.highlight_rules.clear();
                            }
                    }
                });
                ui.menu_button("View", |ui| {
                    if ui.button("Auto Column Width (Ctrl+Shift+W)").clicked() {
                        ui.close_menu();
                        if let Some(tab) = self.active_tab_mut()
                            && let (Some(data), Some(state)) = (&tab.data, &mut tab.viewer_state) {
                                let num_cols = data.headers().len();
                                state.compute_auto_widths(data.as_ref(), num_cols);
                            }
                    }
                    let frozen = self.active_tab().is_some_and(|t| {
                        t.viewer_state.as_ref().is_some_and(|s| s.frozen_cols > 0)
                    });
                    if frozen
                        && ui.button("Unfreeze Columns").clicked() {
                            ui.close_menu();
                            if let Some(tab) = self.active_tab_mut()
                                && let Some(state) = &mut tab.viewer_state { state.frozen_cols = 0; }

                        }
                    // Show All Columns
                    let hidden = self.active_tab().map_or(0, |t| {
                        t.viewer_state.as_ref().map_or(0, |s| s.hidden_col_count())
                    });
                    if hidden > 0
                        && ui.button(format!("Show All Columns ({} hidden)", hidden)).clicked() {
                            ui.close_menu();
                            if let Some(tab) = self.active_tab_mut()
                                && let Some(state) = &mut tab.viewer_state {
                                    state.show_all_columns();
                            }
                    }
                    ui.separator();
                    let ppp = ctx.pixels_per_point();
                    ui.label(format!("Zoom: {:.0}%", ppp * 100.0));
                    if ui.button("Zoom In (Ctrl++)").clicked() {
                        ui.close_menu();
                        ctx.set_pixels_per_point((ppp + 0.1).min(3.0));
                    }
                    if ui.button("Zoom Out (Ctrl+-)").clicked() {
                        ui.close_menu();
                        ctx.set_pixels_per_point((ppp - 0.1).max(0.5));
                    }
                    if ui.button("Reset Zoom (Ctrl+0)").clicked() {
                        ui.close_menu();
                        ctx.set_pixels_per_point(1.0);
                    }
                });
                ui.menu_button("Settings", |ui| {
                    if ui.button("Register \"Open with\" for CSV/Excel...").clicked() {
                        ui.close_menu();
                        self.error_msg = Some("Registering...".to_string());
                        let ctx = ctx.clone();
                        let tx = self.notify_tx.clone();
                        std::thread::spawn(move || {
                            crate::register_file_associations();
                            let _ = tx.send("Registered. Right-click a file > Open with > GridPeek".to_string());
                            ctx.request_repaint();
                        });
                    }
                    if ui.button("Unregister \"Open with\"").clicked() {
                        ui.close_menu();
                        self.error_msg = Some("Removing...".to_string());
                        let ctx = ctx.clone();
                        let tx = self.notify_tx.clone();
                        std::thread::spawn(move || {
                            crate::unregister_file_associations();
                            let _ = tx.send("File associations removed.".to_string());
                            ctx.request_repaint();
                        });
                    }
                });
                ui.menu_button("Help", |ui| {
                    if ui.button("About GridPeek").clicked() {
                        ui.close_menu();
                        self.show_about = true;
                    }
                    ui.separator();
                    if ui.button("GitHub Repository").clicked() {
                        ui.close_menu();
                        ctx.open_url(egui::OpenUrl::new_tab("https://github.com/MrRJSR/GridPeek"));
                    }
                    if ui.button("Report Issue").clicked() {
                        ui.close_menu();
                        ctx.open_url(egui::OpenUrl::new_tab("https://github.com/MrRJSR/GridPeek/issues"));
                    }
                    ui.separator();
                    if ui.button("Support GridPeek (Ko-fi)").clicked() {
                        ui.close_menu();
                        ctx.open_url(egui::OpenUrl::new_tab("https://ko-fi.com/gridpeek"));
                    }
                });
            });

            // Tab bar
            if !self.tabs.is_empty() {
                ui.horizontal(|ui| {
                    let mut close_idx: Option<usize> = None;
                    for (i, tab) in self.tabs.iter().enumerate() {
                        let selected = i == self.active_tab;
                        let title = tab.tab_title();
                        if ui.selectable_label(selected, &title).clicked() {
                            self.active_tab = i;
                        }
                        if ui.small_button("x").clicked() {
                            close_idx = Some(i);
                        }
                        ui.separator();
                    }
                    if let Some(idx) = close_idx {
                        self.close_tab(idx);
                    }
                });
            }

            // Search bar (per active tab)
            if let Some(tab) = self.tabs.get_mut(self.active_tab) {
                if tab.search_visible {
                    ui.horizontal(|ui| {
                        ui.label("Find:");
                        let response = ui.add(
                            egui::TextEdit::singleline(&mut tab.search_query)
                                .desired_width(300.0)
                                .hint_text("Search..."),
                        );
                        if response.changed() { tab.start_search(); }
                        if toggle_search || toggle_replace { response.request_focus(); }

                        let case_btn = ui.add(
                            egui::Button::new(
                                egui::RichText::new("Aa")
                                    .color(if tab.search_case_insensitive {
                                        egui::Color32::from_rgb(0, 120, 215)
                                    } else {
                                        egui::Color32::GRAY
                                    }),
                            ).frame(true),
                        );
                        if case_btn.clicked() { tab.search_case_insensitive = !tab.search_case_insensitive; tab.start_search(); }
                        if case_btn.hovered() { case_btn.on_hover_text("Toggle case-insensitive search"); }

                        let regex_btn = ui.add(
                            egui::Button::new(
                                egui::RichText::new(".*")
                                    .color(if tab.search_regex {
                                        egui::Color32::from_rgb(0, 120, 215)
                                    } else {
                                        egui::Color32::GRAY
                                    }),
                            ).frame(true),
                        );
                        if regex_btn.clicked() { tab.search_regex = !tab.search_regex; tab.start_search(); }
                        if regex_btn.hovered() { regex_btn.on_hover_text("Toggle regex search"); }

                        if let Some(err) = &tab.search_regex_error {
                            ui.colored_label(egui::Color32::RED, err);
                        } else {
                            let result_text = if tab.search_results.is_empty() {
                                if tab.search_done && !tab.search_query.is_empty() { "No results".to_string() }
                                else if !tab.search_query.is_empty() { "Searching...".to_string() }
                                else { String::new() }
                            } else {
                                format!("{} / {}{}", tab.search_index + 1, tab.search_results.len(), if tab.search_done { "" } else { "+" })
                            };
                            ui.label(&result_text);
                        }

                        if ui.button("▼").clicked() { tab.navigate_search(true); }
                        if ui.button("▲").clicked() { tab.navigate_search(false); }
                        if ui.button("✕").clicked() {
                            tab.search_visible = false;
                            tab.replace_visible = false;
                            tab.cancel_search();
                            if let Some(state) = &mut tab.viewer_state { state.highlight_row = None; }
                        }
                    });

                    if tab.replace_visible {
                        ui.horizontal(|ui| {
                            ui.label("Replace:");
                            ui.add(
                                egui::TextEdit::singleline(&mut tab.replace_text)
                                    .desired_width(300.0)
                                    .hint_text("Replace with..."),
                            );
                            if ui.button("Replace").clicked() {
                                tab.replace_current();
                            }
                            if ui.button("Replace All").clicked() {
                                tab.replace_all();
                            }
                        });
                    }
                }

                // Go-to-row dialog
                if tab.goto_visible {
                    let mut jump_row: Option<usize> = None;
                    ui.horizontal(|ui| {
                        ui.label("Go to row:");
                        let response = ui.add(
                            egui::TextEdit::singleline(&mut tab.goto_text)
                                .desired_width(100.0)
                                .hint_text("Row #"),
                        );
                        if goto { response.request_focus(); }
                        if (ui.button("Go").clicked() || ui.input(|i| i.key_pressed(egui::Key::Enter)))
                            && let Ok(n) = tab.goto_text.trim().parse::<usize>() { jump_row = Some(n); }

                        if ui.button("✕").clicked() { tab.goto_visible = false; }
                    });
                    if let Some(target) = jump_row {
                        if let Some(state) = &mut tab.viewer_state {
                            let actual = target.saturating_sub(1);
                            let display_row = state.filtered_indices.iter()
                                .position(|&idx| idx as usize == actual)
                                .unwrap_or(actual.min(state.display_row_count().saturating_sub(1)));
                            let num_cols = tab.data.as_ref().map_or(1, |d| d.headers().len());
                            state.jump_cursor(display_row, state.cursor.map_or(0, |c| c.col), false, num_cols);
                        }
                        tab.goto_visible = false;
                    }
                }

                // Sheet tabs for Excel
                let mut switch_to_sheet: Option<usize> = None;
                if let Some(data) = &tab.data
                    && data.sheet_count() > 1 {
                        let names = data.sheet_names();
                        ui.horizontal(|ui| {
                            for (i, name) in names.iter().enumerate() {
                                let selected = i == tab.current_sheet;
                                if ui.selectable_label(selected, name).clicked() && !selected {
                                    switch_to_sheet = Some(i);
                                }
                            }
                        });
                }
                if let Some(sheet_idx) = switch_to_sheet {
                    tab.current_sheet = sheet_idx;
                    if let Some(data) = &mut tab.data {
                        data.switch_sheet(sheet_idx);
                        let num_cols = data.headers().len();
                        tab.viewer_state = Some(ViewerState::new(num_cols));
                    }
                    tab.raw_data_cache = None;
                    tab.synthetic_row_offsets = None;
                    tab.cancel_search();
                    tab.search_results.clear();
                    tab.build_synthetic_search_buffer();
                }

                // File path + encoding
                if let Some(path) = &tab.file_path {
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new(path.display().to_string()).weak().small());
                        if let Some(data) = &tab.data {
                            ui.label(egui::RichText::new(format!("[{}]", data.encoding())).weak().small());
                        }
                    });
                }
            }
        });

        // Central panel
        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(error) = &self.error_msg {
                ui.colored_label(egui::Color32::RED, error);
            }

            if let Some(tab) = self.tabs.get_mut(self.active_tab) {
                if let (Some(data), Some(state)) = (&tab.data, &mut tab.viewer_state) {
                    viewer::draw_table(ui, data.as_ref(), state);
                    state.update_selection_stats(data.as_ref());
                }
            } else if self.error_msg.is_none() {
                ui.vertical_centered(|ui| {
                    ui.add_space(100.0);
                    ui.heading("GridPeek");
                    ui.label("Ultra-fast CSV / Excel / TSV Viewer");
                    ui.add_space(20.0);
                    ui.label("Drop a file here or press Ctrl+O to open");
                    ui.add_space(10.0);
                    if ui.button("Open File...").clicked() { self.open_file_dialog(); }
                });
            }
        });

        // About dialog
        if self.show_about {
            let mut open = true;
            egui::Window::new("About GridPeek")
                .open(&mut open)
                .resizable(false)
                .collapsible(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.heading("GridPeek");
                        ui.label(format!("Version {}", env!("CARGO_PKG_VERSION")));
                        ui.add_space(8.0);
                        ui.label("Ultra-fast CSV / Excel / TSV Viewer");
                        ui.label("Lightweight, zero-dependency data inspection tool");
                        ui.add_space(8.0);
                        ui.label("MIT License");
                    });
                });
            if !open {
                self.show_about = false;
            }
        }

        // Export settings dialog
        if self.export_settings.show {
            let mut do_export = false;
            let mut close_dialog = false;
            let settings = &mut self.export_settings;

            egui::Window::new("Export Settings")
                .resizable(false)
                .collapsible(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    // --- Columns ---
                    ui.label(egui::RichText::new("Columns:").strong());
                    ui.horizontal_wrapped(|ui| {
                        for (i, name) in settings.col_names.iter().enumerate() {
                            if i < settings.col_selected.len() {
                                ui.checkbox(&mut settings.col_selected[i], name);
                            }
                        }
                    });
                    ui.horizontal(|ui| {
                        if ui.button("Select All").clicked() {
                            for v in settings.col_selected.iter_mut() { *v = true; }
                        }
                        if ui.button("Deselect All").clicked() {
                            for v in settings.col_selected.iter_mut() { *v = false; }
                        }
                    });

                    ui.add_space(8.0);

                    // --- Rows ---
                    ui.label(egui::RichText::new("Rows:").strong());
                    ui.radio_value(
                        &mut settings.row_mode,
                        RowExportMode::AllFiltered,
                        format!("All filtered rows ({})", settings.total_filtered),
                    );
                    let sel_label = if settings.has_selection {
                        format!("Selection only ({} rows)", settings.selection_rows)
                    } else {
                        "Selection only (no selection)".to_string()
                    };
                    ui.add_enabled_ui(settings.has_selection, |ui| {
                        ui.radio_value(
                            &mut settings.row_mode,
                            RowExportMode::SelectionOnly,
                            sel_label,
                        );
                    });
                    ui.horizontal(|ui| {
                        ui.radio_value(
                            &mut settings.row_mode,
                            RowExportMode::RowRange,
                            "Row range:",
                        );
                        ui.add(
                            egui::TextEdit::singleline(&mut settings.range_from)
                                .desired_width(60.0)
                                .hint_text("from"),
                        );
                        ui.label("to");
                        ui.add(
                            egui::TextEdit::singleline(&mut settings.range_to)
                                .desired_width(60.0)
                                .hint_text("to"),
                        );
                    });

                    ui.add_space(8.0);

                    // --- Row limit & sampling ---
                    ui.horizontal(|ui| {
                        ui.label("Row limit:");
                        ui.add(
                            egui::TextEdit::singleline(&mut settings.row_limit)
                                .desired_width(80.0)
                                .hint_text("no limit"),
                        );
                    });
                    ui.horizontal(|ui| {
                        ui.label("Sampling: every");
                        ui.add(
                            egui::TextEdit::singleline(&mut settings.sampling_every)
                                .desired_width(60.0)
                                .hint_text("1"),
                        );
                        ui.label("rows (1 = all)");
                    });

                    ui.add_space(8.0);

                    // --- Preview ---
                    let selected_cols = settings.col_selected.iter().filter(|&&v| v).count();
                    let base_rows = match settings.row_mode {
                        RowExportMode::AllFiltered => settings.total_filtered,
                        RowExportMode::SelectionOnly => settings.selection_rows,
                        RowExportMode::RowRange => {
                            let from = settings.range_from.parse::<usize>().unwrap_or(1).max(1);
                            let to = settings.range_to.parse::<usize>().unwrap_or(settings.total_filtered);
                            to.saturating_sub(from) + 1
                        }
                    };
                    let sampling = settings.sampling_every.parse::<usize>().unwrap_or(1).max(1);
                    let after_sampling = base_rows.div_ceil(sampling);
                    let limit = settings.row_limit.parse::<usize>().ok();
                    let final_rows = match limit {
                        Some(l) => after_sampling.min(l),
                        None => after_sampling,
                    };

                    // Estimate file size (rough: avg ~20 bytes per cell for CSV)
                    let est_cells = final_rows as u64 * selected_cols as u64;
                    let est_bytes = est_cells * 20; // rough average
                    let size_str = if est_bytes < 1024 {
                        format!("{} B", est_bytes)
                    } else if est_bytes < 1024 * 1024 {
                        format!("{:.1} KB", est_bytes as f64 / 1024.0)
                    } else {
                        format!("{:.1} MB", est_bytes as f64 / (1024.0 * 1024.0))
                    };

                    ui.label(format!(
                        "Preview: ~{} rows x {} columns  (est. ~{})",
                        final_rows, selected_cols, size_str
                    ));

                    ui.add_space(8.0);

                    // --- Buttons ---
                    ui.horizontal(|ui| {
                        if ui.button("Export...").clicked() {
                            do_export = true;
                        }
                        if ui.button("Cancel").clicked() {
                            close_dialog = true;
                        }
                    });
                });

            if close_dialog {
                self.export_settings.show = false;
            }
            if do_export {
                self.do_export_with_settings();
            }
        }

        // Request repaint if search is running
        let needs_repaint = self.active_tab().is_some_and(|t| {
            t.search_handle.is_some() && !t.search_done
        });
        if needs_repaint { ctx.request_repaint(); }
    }
}

fn configure_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();

    let font_paths: &[&str] = &[
        "C:\\Windows\\Fonts\\YuGothM.ttc",
        "C:\\Windows\\Fonts\\yugothic.ttf",
        "C:\\Windows\\Fonts\\meiryo.ttc",
        "C:\\Windows\\Fonts\\msgothic.ttc",
        "C:\\Windows\\Fonts\\msmincho.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/OTF/NotoSansCJK-Regular.ttc",
    ];

    for path in font_paths {
        if let Ok(font_data) = std::fs::read(path) {
            fonts.font_data.insert(
                "japanese".to_owned(),
                egui::FontData::from_owned(font_data).into(),
            );
            fonts.families.entry(egui::FontFamily::Proportional).or_default().push("japanese".to_owned());
            fonts.families.entry(egui::FontFamily::Monospace).or_default().push("japanese".to_owned());
            break;
        }
    }

    ctx.set_fonts(fonts);
}
