use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use eframe::egui;
use egui_extras::{Column, TableBuilder};

use crate::loader::DataSource;

/// Sort state for a column
#[derive(Clone, Copy, PartialEq)]
pub enum SortOrder {
    None,
    Ascending,
    Descending,
}

/// Cell position
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CellPos {
    pub row: usize,
    pub col: usize,
}

/// Undo entry for cell edits
#[derive(Clone, Debug)]
pub struct UndoEntry {
    pub row: usize, // actual row
    pub col: usize,
    pub old_value: Option<String>, // None = was not in overlay (original data)
}

/// Column statistics (computed on demand)
#[derive(Clone, Debug)]
pub struct ColumnStats {
    pub col: usize,
    pub count: usize,
    pub non_empty: usize,
    pub unique: usize,
    pub numeric_count: usize,
    pub sum: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub top_values: Vec<(String, usize)>,
}

impl ColumnStats {
    pub fn avg(&self) -> Option<f64> {
        if self.numeric_count > 0 {
            Some(self.sum / self.numeric_count as f64)
        } else {
            None
        }
    }
}

// --- Conditional highlight types ---

#[derive(Clone, Copy, PartialEq)]
pub enum HighlightOp {
    Equals,
    Contains,
    GreaterThan,
    LessThan,
}

impl HighlightOp {
    pub fn label(&self) -> &'static str {
        match self {
            HighlightOp::Equals => "Equals",
            HighlightOp::Contains => "Contains",
            HighlightOp::GreaterThan => ">",
            HighlightOp::LessThan => "<",
        }
    }

    pub const ALL: [HighlightOp; 4] = [
        HighlightOp::Equals,
        HighlightOp::Contains,
        HighlightOp::GreaterThan,
        HighlightOp::LessThan,
    ];
}

#[derive(Clone)]
pub struct HighlightRule {
    pub col: Option<usize>, // None = all columns
    pub op: HighlightOp,
    pub value: String,
    pub color: egui::Color32,
}

pub const HIGHLIGHT_COLORS: [egui::Color32; 6] = [
    egui::Color32::from_rgba_premultiplied(200, 200, 0, 60),   // yellow
    egui::Color32::from_rgba_premultiplied(0, 200, 0, 60),     // green
    egui::Color32::from_rgba_premultiplied(255, 140, 0, 60),   // orange
    egui::Color32::from_rgba_premultiplied(0, 120, 255, 60),   // blue
    egui::Color32::from_rgba_premultiplied(220, 50, 50, 60),   // red
    egui::Color32::from_rgba_premultiplied(180, 0, 220, 60),   // purple
];

impl HighlightRule {
    pub fn matches(&self, cell_value: &str, actual_col: usize) -> bool {
        // Check column constraint
        if let Some(c) = self.col
            && c != actual_col {
                return false;
        }
        match self.op {
            HighlightOp::Equals => cell_value == self.value,
            HighlightOp::Contains => cell_value.contains(&self.value),
            HighlightOp::GreaterThan => {
                if let (Ok(a), Ok(b)) = (cell_value.parse::<f64>(), self.value.parse::<f64>()) {
                    a > b
                } else {
                    cell_value > self.value.as_str()
                }
            }
            HighlightOp::LessThan => {
                if let (Ok(a), Ok(b)) = (cell_value.parse::<f64>(), self.value.parse::<f64>()) {
                    a < b
                } else {
                    cell_value < self.value.as_str()
                }
            }
        }
    }
}

/// Viewer state
pub struct ViewerState {
    /// Column sort state
    pub sort_col: Option<usize>,
    pub sort_order: SortOrder,
    /// Sorted row indices (maps display row → actual row)
    pub sorted_indices: Vec<u32>,
    /// Filter text per column
    pub filters: Vec<String>,
    /// Filtered row indices (after applying filters)
    pub filtered_indices: Vec<u32>,
    /// Whether filters are active
    pub filters_active: bool,
    /// Selection anchor
    pub selection_anchor: Option<CellPos>,
    /// Selection end
    pub selection_end: Option<CellPos>,
    /// Row to scroll to (set by search)
    pub scroll_to_row: Option<usize>,
    /// Highlighted row for search results
    pub highlight_row: Option<usize>,
    /// Show filter row
    pub show_filters: bool,
    /// Whether indices need rebuild
    indices_dirty: bool,

    // --- Cell editing overlay ---
    pub edits: HashMap<(usize, usize), String>,
    pub modified: bool,
    /// Currently editing cell (display_row, col) and its text buffer
    pub editing_cell: Option<(usize, usize, String)>,

    // --- Undo ---
    pub undo_stack: Vec<UndoEntry>,

    // --- Keyboard navigation ---
    /// The "cursor" cell position (display row, col) - used for keyboard nav
    pub cursor: Option<CellPos>,

    // --- Auto column widths ---
    pub auto_widths: Option<Vec<f32>>,

    // --- Column freeze ---
    pub frozen_cols: usize,

    // --- Column statistics popup ---
    pub stats_popup: Option<ColumnStats>,

    // --- Status bar selection stats ---
    pub status_sum: Option<f64>,
    pub status_avg: Option<f64>,
    pub status_count: usize,
    pub status_numeric_count: usize,

    // --- Duplicate row highlighting ---
    pub highlight_duplicates: bool,
    pub duplicate_rows: HashSet<usize>,
    pub duplicate_count: usize,

    // --- Column visibility/reorder ---
    pub visible_cols: Vec<usize>,
    pub num_actual_cols: usize,

    // --- Conditional highlight ---
    pub highlight_rules: Vec<HighlightRule>,
    pub show_highlight_dialog: bool,
    pub highlight_dialog_col: usize,
    pub highlight_dialog_op: HighlightOp,
    pub highlight_dialog_value: String,
    pub highlight_dialog_color_idx: usize,
}

impl ViewerState {
    pub fn new(num_cols: usize) -> Self {
        ViewerState {
            sort_col: None,
            sort_order: SortOrder::None,
            sorted_indices: Vec::new(),
            filters: vec![String::new(); num_cols],
            filtered_indices: Vec::new(),
            filters_active: false,
            selection_anchor: None,
            selection_end: None,
            scroll_to_row: None,
            highlight_row: None,
            show_filters: false,
            indices_dirty: true,
            edits: HashMap::new(),
            modified: false,
            editing_cell: None,
            undo_stack: Vec::new(),
            cursor: None,
            auto_widths: None,
            frozen_cols: 0,
            stats_popup: None,
            status_sum: None,
            status_avg: None,
            status_count: 0,
            status_numeric_count: 0,
            highlight_duplicates: false,
            duplicate_rows: HashSet::new(),
            duplicate_count: 0,
            visible_cols: (0..num_cols).collect(),
            num_actual_cols: num_cols,
            highlight_rules: Vec::new(),
            show_highlight_dialog: false,
            highlight_dialog_col: 0,
            highlight_dialog_op: HighlightOp::Equals,
            highlight_dialog_value: String::new(),
            highlight_dialog_color_idx: 0,
        }
    }

    /// Initialize indices from data source
    pub fn init_indices(&mut self, row_count: usize) {
        if self.indices_dirty {
            self.sorted_indices = (0..row_count as u32).collect();
            self.filtered_indices = self.sorted_indices.clone();
            self.indices_dirty = false;
        }
    }

    /// Get cell value, checking edit overlay first
    pub fn get_cell<'a>(&'a self, data: &'a dyn DataSource, actual_row: usize, col: usize) -> &'a str {
        if let Some(val) = self.edits.get(&(actual_row, col)) {
            val.as_str()
        } else {
            data.cell(actual_row, col)
        }
    }

    /// Commit an edit to a cell (pushes undo)
    pub fn commit_edit(&mut self, actual_row: usize, col: usize, new_value: String) {
        let old_value = self.edits.get(&(actual_row, col)).cloned();
        self.undo_stack.push(UndoEntry {
            row: actual_row,
            col,
            old_value,
        });
        self.edits.insert((actual_row, col), new_value);
        self.modified = true;
    }

    /// Undo last edit
    pub fn undo(&mut self) {
        if let Some(entry) = self.undo_stack.pop() {
            match entry.old_value {
                Some(old) => {
                    self.edits.insert((entry.row, entry.col), old);
                }
                None => {
                    self.edits.remove(&(entry.row, entry.col));
                }
            }
            if self.edits.is_empty() {
                self.modified = false;
            }
        }
    }

    /// Toggle sort on a column
    pub fn toggle_sort(&mut self, col: usize, data: &dyn DataSource) {
        if self.sort_col == Some(col) {
            self.sort_order = match self.sort_order {
                SortOrder::None => SortOrder::Ascending,
                SortOrder::Ascending => SortOrder::Descending,
                SortOrder::Descending => SortOrder::None,
            };
        } else {
            self.sort_col = Some(col);
            self.sort_order = SortOrder::Ascending;
        }

        self.apply_sort(data);
        self.apply_filters(data);
    }

    /// Apply current sort to indices
    fn apply_sort(&mut self, data: &dyn DataSource) {
        let row_count = data.row_count();
        self.sorted_indices = (0..row_count as u32).collect();

        if let (Some(col), order) = (self.sort_col, self.sort_order) {
            if order == SortOrder::None {
                return;
            }

            let mut keys: Vec<(u32, String)> = self
                .sorted_indices
                .iter()
                .map(|&idx| {
                    let val = self.get_cell(data, idx as usize, col).to_string();
                    (idx, val)
                })
                .collect();

            match order {
                SortOrder::Ascending => {
                    keys.sort_by(|a, b| smart_cmp(&a.1, &b.1));
                }
                SortOrder::Descending => {
                    keys.sort_by(|a, b| smart_cmp(&b.1, &a.1));
                }
                SortOrder::None => {}
            }

            self.sorted_indices = keys.into_iter().map(|(idx, _)| idx).collect();
        }
    }

    /// Apply filters and rebuild filtered_indices
    pub fn apply_filters(&mut self, data: &dyn DataSource) {
        self.filters_active = self.filters.iter().any(|f| !f.is_empty());

        if !self.filters_active {
            self.filtered_indices = self.sorted_indices.clone();
            return;
        }

        let active_filters: Vec<(usize, String)> = self
            .filters
            .iter()
            .enumerate()
            .filter(|(_, f)| !f.is_empty())
            .map(|(i, f)| (i, f.to_lowercase()))
            .collect();

        self.filtered_indices = self
            .sorted_indices
            .iter()
            .copied()
            .filter(|&row_idx| {
                active_filters.iter().all(|(col, filter_text)| {
                    let cell_val = self.get_cell(data, row_idx as usize, *col);
                    cell_val.to_lowercase().contains(filter_text.as_str())
                })
            })
            .collect();
    }

    /// Get the display row count
    pub fn display_row_count(&self) -> usize {
        self.filtered_indices.len()
    }

    /// Map display row to actual data row
    pub fn actual_row(&self, display_row: usize) -> usize {
        self.filtered_indices
            .get(display_row)
            .map(|&idx| idx as usize)
            .unwrap_or(0)
    }

    /// Get selected range (normalized: top-left to bottom-right)
    pub fn selection_range(&self) -> Option<(CellPos, CellPos)> {
        match (self.selection_anchor, self.selection_end) {
            (Some(a), Some(b)) => {
                let min_row = a.row.min(b.row);
                let max_row = a.row.max(b.row);
                let min_col = a.col.min(b.col);
                let max_col = a.col.max(b.col);
                Some((
                    CellPos {
                        row: min_row,
                        col: min_col,
                    },
                    CellPos {
                        row: max_row,
                        col: max_col,
                    },
                ))
            }
            (Some(a), None) => Some((a, a)),
            _ => None,
        }
    }

    /// Check if a cell is in the current selection
    pub fn is_selected(&self, display_row: usize, col: usize) -> bool {
        if let Some((tl, br)) = self.selection_range() {
            display_row >= tl.row
                && display_row <= br.row
                && col >= tl.col
                && col <= br.col
        } else {
            false
        }
    }

    /// Copy selected cells to clipboard as tab-separated text
    /// col coordinates are display-col; we convert via visible_cols
    pub fn copy_selection(&self, data: &dyn DataSource) -> String {
        let Some((tl, br)) = self.selection_range() else {
            return String::new();
        };

        let mut result = String::new();
        for display_row in tl.row..=br.row {
            if display_row >= self.filtered_indices.len() {
                break;
            }
            let actual = self.actual_row(display_row);
            for dc in tl.col..=br.col {
                if dc > tl.col {
                    result.push('\t');
                }
                let actual_col = self.visible_cols.get(dc).copied().unwrap_or(dc);
                result.push_str(self.get_cell(data, actual, actual_col));
            }
            result.push('\n');
        }
        result
    }

    /// Move cursor by delta, optionally extending selection
    pub fn move_cursor(&mut self, dr: i32, dc: i32, extend: bool, _num_actual_cols: usize) {
        let display_rows = self.display_row_count();
        let num_cols = self.visible_cols.len();
        if display_rows == 0 || num_cols == 0 {
            return;
        }

        let cur = self.cursor.unwrap_or(CellPos { row: 0, col: 0 });
        let new_row = (cur.row as i64 + dr as i64).clamp(0, display_rows as i64 - 1) as usize;
        let new_col = (cur.col as i64 + dc as i64).clamp(0, num_cols as i64 - 1) as usize;
        let new_pos = CellPos { row: new_row, col: new_col };

        self.cursor = Some(new_pos);

        if extend {
            self.selection_end = Some(new_pos);
        } else {
            self.selection_anchor = Some(new_pos);
            self.selection_end = Some(new_pos);
        }

        self.scroll_to_row = Some(new_row);
    }

    /// Jump cursor to a specific position
    pub fn jump_cursor(&mut self, row: usize, col: usize, extend: bool, _num_actual_cols: usize) {
        let display_rows = self.display_row_count();
        let num_cols = self.visible_cols.len();
        if display_rows == 0 || num_cols == 0 {
            return;
        }
        let row = row.min(display_rows.saturating_sub(1));
        let col = col.min(num_cols.saturating_sub(1));
        let pos = CellPos { row, col };
        self.cursor = Some(pos);
        if extend {
            self.selection_end = Some(pos);
        } else {
            self.selection_anchor = Some(pos);
            self.selection_end = Some(pos);
        }
        self.scroll_to_row = Some(row);
    }

    /// Compute auto column widths by sampling first 100 rows
    pub fn compute_auto_widths(&mut self, data: &dyn DataSource, _num_actual_cols: usize) {
        let char_width = 8.0_f32;
        let padding = 16.0_f32;
        let min_width = 40.0_f32;
        let max_width = 400.0_f32;

        let num_cols = self.num_actual_cols;
        let mut widths = vec![min_width; num_cols];

        let headers = data.headers();
        for (i, h) in headers.iter().enumerate() {
            if i < num_cols {
                widths[i] = widths[i].max((h.len() as f32 * char_width + padding).min(max_width));
            }
        }

        let sample = self.display_row_count().min(100);
        for dr in 0..sample {
            let actual = self.actual_row(dr);
            for (col, width) in widths.iter_mut().enumerate().take(num_cols) {
                let val = self.get_cell(data, actual, col);
                let w = (val.len() as f32 * char_width + padding).min(max_width);
                *width = width.max(w);
            }
        }

        self.auto_widths = Some(widths);
    }

    /// Compute column statistics for a given actual column (uses filtered rows)
    pub fn compute_column_stats(&mut self, data: &dyn DataSource, col: usize) {
        let mut count = 0usize;
        let mut non_empty = 0usize;
        let mut numeric_count = 0usize;
        let mut sum = 0.0f64;
        let mut min: Option<f64> = None;
        let mut max: Option<f64> = None;
        let mut unique_set = HashSet::new();
        let mut value_counts: HashMap<String, usize> = HashMap::new();

        for &row_idx in &self.filtered_indices {
            let val = self.get_cell(data, row_idx as usize, col);
            count += 1;
            if !val.is_empty() {
                non_empty += 1;
                unique_set.insert(val.to_string());
                *value_counts.entry(val.to_string()).or_insert(0) += 1;
                if let Ok(n) = val.parse::<f64>() {
                    numeric_count += 1;
                    sum += n;
                    min = Some(min.map_or(n, |m: f64| m.min(n)));
                    max = Some(max.map_or(n, |m: f64| m.max(n)));
                }
            }
        }

        let mut top_values: Vec<(String, usize)> = value_counts.into_iter().collect();
        top_values.sort_by(|a, b| b.1.cmp(&a.1));
        top_values.truncate(10);

        self.stats_popup = Some(ColumnStats {
            col,
            count,
            non_empty,
            unique: unique_set.len(),
            numeric_count,
            sum,
            min,
            max,
            top_values,
        });
    }

    /// Compute which rows are duplicates (based on all cell values)
    pub fn compute_duplicate_rows(&mut self, data: &dyn DataSource) {
        let num_cols = data.headers().len();
        let mut seen: HashMap<u64, Vec<usize>> = HashMap::new();

        for &row_idx in &self.filtered_indices {
            let actual = row_idx as usize;
            let mut hasher = std::hash::DefaultHasher::new();
            for col in 0..num_cols {
                let val = self.get_cell(data, actual, col);
                val.hash(&mut hasher);
                0xFFu8.hash(&mut hasher);
            }
            let h = hasher.finish();
            seen.entry(h).or_default().push(actual);
        }

        self.duplicate_rows.clear();
        self.duplicate_count = 0;
        for rows in seen.values() {
            if rows.len() > 1 {
                self.duplicate_count += rows.len();
                for &r in rows {
                    self.duplicate_rows.insert(r);
                }
            }
        }
    }

    /// Update status bar selection stats (using visible_cols mapping)
    pub fn update_selection_stats(&mut self, data: &dyn DataSource) {
        self.status_sum = None;
        self.status_avg = None;
        self.status_count = 0;
        self.status_numeric_count = 0;

        let Some((tl, br)) = self.selection_range() else {
            return;
        };

        let sel_cells = (br.row - tl.row + 1) * (br.col - tl.col + 1);
        if sel_cells > 100_000 || sel_cells <= 1 {
            return;
        }

        let mut sum = 0.0f64;
        let mut count = 0usize;
        let mut numeric = 0usize;

        for dr in tl.row..=br.row {
            if dr >= self.filtered_indices.len() {
                break;
            }
            let actual = self.actual_row(dr);
            for dc in tl.col..=br.col {
                let actual_col = self.visible_cols.get(dc).copied().unwrap_or(dc);
                let val = self.get_cell(data, actual, actual_col);
                if !val.is_empty() {
                    count += 1;
                    if let Ok(n) = val.parse::<f64>() {
                        numeric += 1;
                        sum += n;
                    }
                }
            }
        }

        self.status_count = count;
        self.status_numeric_count = numeric;
        if numeric > 0 {
            self.status_sum = Some(sum);
            self.status_avg = Some(sum / numeric as f64);
        }
    }

    // --- Column visibility helpers ---

    pub fn hide_column(&mut self, actual_col: usize) {
        self.visible_cols.retain(|&c| c != actual_col);
    }

    pub fn show_all_columns(&mut self) {
        self.visible_cols = (0..self.num_actual_cols).collect();
    }

    pub fn hidden_col_count(&self) -> usize {
        self.num_actual_cols - self.visible_cols.len()
    }

    pub fn move_column_left(&mut self, display_idx: usize) {
        if display_idx > 0 && display_idx < self.visible_cols.len() {
            self.visible_cols.swap(display_idx, display_idx - 1);
        }
    }

    pub fn move_column_right(&mut self, display_idx: usize) {
        if display_idx + 1 < self.visible_cols.len() {
            self.visible_cols.swap(display_idx, display_idx + 1);
        }
    }

    /// Find the first matching highlight color for a cell
    pub fn highlight_color_for(&self, cell_value: &str, actual_col: usize) -> Option<egui::Color32> {
        for rule in &self.highlight_rules {
            if rule.matches(cell_value, actual_col) {
                return Some(rule.color);
            }
        }
        None
    }
}

/// Smart comparison: try numeric first, then string
fn smart_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(na), Ok(nb)) = (a.parse::<f64>(), b.parse::<f64>()) {
        return na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal);
    }
    a.cmp(b)
}

/// Draw the table viewer
pub fn draw_table(
    ui: &mut egui::Ui,
    data: &dyn DataSource,
    state: &mut ViewerState,
) {
    let headers = data.headers();
    let num_actual_cols = headers.len();
    if num_actual_cols == 0 {
        ui.label("No data loaded");
        return;
    }

    state.init_indices(data.row_count());

    let display_rows = state.display_row_count();

    // Filter row (show filters for visible cols only)
    if state.show_filters {
        let filter_vis_cols: Vec<usize> = state.visible_cols.clone();
        ui.horizontal(|ui| {
            ui.label("Filters:");
            let mut filters_changed = false;
            for &actual_col in filter_vis_cols.iter() {
                if actual_col < state.filters.len() {
                    let response = ui.add(
                        egui::TextEdit::singleline(&mut state.filters[actual_col])
                            .desired_width(100.0)
                            .hint_text(&headers[actual_col]),
                    );
                    if response.changed() {
                        filters_changed = true;
                    }
                }
            }
            if filters_changed {
                state.apply_filters(data);
            }
        });
        ui.separator();
    }

    // Status bar
    ui.horizontal(|ui| {
        ui.label(format!(
            "Rows: {} / {}",
            display_rows,
            data.row_count()
        ));
        if state.highlight_duplicates && state.duplicate_count > 0 {
            ui.label(
                egui::RichText::new(format!("Duplicates: {}", state.duplicate_count))
                    .color(egui::Color32::from_rgb(200, 60, 60)),
            );
        }
        if let Some((tl, br)) = state.selection_range() {
            let sel_rows = br.row - tl.row + 1;
            let sel_cols = br.col - tl.col + 1;
            ui.label(format!("Selection: {}x{}", sel_rows, sel_cols));

            if state.status_count > 1 {
                ui.separator();
                ui.label(format!("Count: {}", state.status_count));
                if let Some(sum) = state.status_sum {
                    ui.label(format!("Sum: {:.4}", sum));
                }
                if let Some(avg) = state.status_avg {
                    ui.label(format!("Avg: {:.4}", avg));
                }
            }
        }

        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            let delim = data.delimiter_name();
            if !delim.is_empty() {
                ui.label(egui::RichText::new(delim).weak().small());
            }
            if let Some(size) = data.file_size() {
                let size_str = if size < 1024 {
                    format!("{} B", size)
                } else if size < 1024 * 1024 {
                    format!("{:.1} KB", size as f64 / 1024.0)
                } else {
                    format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
                };
                ui.label(egui::RichText::new(size_str).weak().small());
            }
            if state.hidden_col_count() > 0 {
                ui.label(egui::RichText::new(format!("{} cols hidden", state.hidden_col_count())).weak().small());
            }
        });
    });

    // Build table with horizontal scroll
    let available = ui.available_size();
    let text_height = ui.text_style_height(&egui::TextStyle::Body);
    let row_height = text_height + 4.0;

    // Snapshot visible_cols for use inside closures
    let vis_cols_snapshot: Vec<usize> = state.visible_cols.clone();

    let total_width: f32 = 50.0 + vis_cols_snapshot.iter().map(|&ac| {
        state.auto_widths.as_ref().and_then(|w| w.get(ac).copied()).unwrap_or(120.0)
    }).sum::<f32>();
    let need_hscroll = total_width > available.x;

    let hscroll_id = ui.id().with("table_hscroll");
    egui::ScrollArea::horizontal()
        .id_salt(hscroll_id)
        .auto_shrink([false, false])
        .show(ui, |ui| {

    if need_hscroll {
        ui.set_min_width(total_width + 20.0);
    }

    let mut table = TableBuilder::new(ui)
        .striped(true)
        .resizable(true)
        .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
        .min_scrolled_height(0.0)
        .max_scroll_height(available.y - 30.0);

    // Row number column
    table = table.column(Column::auto().at_least(50.0));

    // Data columns (visible only)
    for &actual_col in &vis_cols_snapshot {
        let initial_w = state
            .auto_widths
            .as_ref()
            .and_then(|w| w.get(actual_col).copied())
            .unwrap_or(120.0);

        let col_spec = Column::initial(initial_w).at_least(40.0).clip(true);
        table = table.column(col_spec);
    }

    if let Some(target_row) = state.scroll_to_row.take() {
        table = table.scroll_to_row(target_row, Some(egui::Align::Center));
    }

    let mut commit_edit: Option<(usize, usize, String)> = None;
    let mut start_edit: Option<(usize, usize, String)> = None;
    let mut compute_stats_col: Option<usize> = None;
    let mut toggle_freeze_col: Option<usize> = None;
    let mut hide_col: Option<usize> = None;
    let mut move_col_left: Option<usize> = None;
    let mut move_col_right: Option<usize> = None;
    let mut add_highlight_rule: Option<HighlightRule> = None;

    table
        .header(row_height + 4.0, |mut header| {
            // Row number header
            header.col(|ui| {
                ui.strong("#");
            });
            // Data column headers (visible only)
            for (display_idx, &actual_col) in vis_cols_snapshot.iter().enumerate() {
                header.col(|ui| {
                    let col_name = headers.get(actual_col).map_or("?", |s| s.as_str());
                    let sort_indicator = if state.sort_col == Some(actual_col) {
                        match state.sort_order {
                            SortOrder::Ascending => " ▲",
                            SortOrder::Descending => " ▼",
                            SortOrder::None => "",
                        }
                    } else {
                        ""
                    };
                    let freeze_indicator = if display_idx < state.frozen_cols { "❄ " } else { "" };
                    let label = format!("{}{}{}", freeze_indicator, col_name, sort_indicator);
                    let response = ui
                        .add(egui::Label::new(egui::RichText::new(&label).strong()).sense(egui::Sense::click()));

                    if response.clicked() {
                        state.toggle_sort(actual_col, data);
                    }

                    response.context_menu(|ui| {
                        // Freeze
                        if display_idx < state.frozen_cols {
                            if ui.button("Unfreeze columns").clicked() {
                                toggle_freeze_col = Some(0);
                                ui.close_menu();
                            }
                        } else {
                            if ui.button(format!("Freeze up to \"{}\"", col_name)).clicked() {
                                toggle_freeze_col = Some(display_idx + 1);
                                ui.close_menu();
                            }
                        }
                        ui.separator();
                        // Hide column
                        if ui.button("Hide column").clicked() {
                            hide_col = Some(actual_col);
                            ui.close_menu();
                        }
                        // Move left/right
                        if display_idx > 0
                            && ui.button("Move left").clicked() {
                                move_col_left = Some(display_idx);
                                ui.close_menu();
                        }
                        if display_idx + 1 < vis_cols_snapshot.len()
                            && ui.button("Move right").clicked() {
                                move_col_right = Some(display_idx);
                                ui.close_menu();
                        }
                        ui.separator();
                        if ui.button("Column statistics...").clicked() {
                            compute_stats_col = Some(actual_col);
                            ui.close_menu();
                        }
                    });
                });
            }
        })
        .body(|body| {
            body.rows(row_height, display_rows, |mut row| {
                let display_row = row.index();
                let actual_row = state.actual_row(display_row);

                let is_highlight = state.highlight_row == Some(display_row);
                let is_cursor_row = state.cursor.is_some_and(|c| c.row == display_row);

                // Row number
                let is_duplicate = state.highlight_duplicates && state.duplicate_rows.contains(&actual_row);
                row.col(|ui| {
                    if is_duplicate {
                        let rect = ui.available_rect_before_wrap();
                        ui.painter().rect_filled(
                            rect,
                            0.0,
                            egui::Color32::from_rgba_premultiplied(200, 60, 60, 50),
                        );
                    }
                    ui.label(
                        egui::RichText::new(format!("{}", actual_row + 1))
                            .weak()
                            .monospace(),
                    );
                });

                // Data cells (visible cols only)
                for (display_col, &actual_col) in vis_cols_snapshot.iter().enumerate() {
                    row.col(|ui| {
                        let is_sel = state.is_selected(display_row, display_col);
                        let is_cursor = is_cursor_row && state.cursor.is_some_and(|c| c.col == display_col);

                        let is_editing = state.editing_cell.as_ref().is_some_and(|(r, c, _)| {
                            *r == display_row && *c == display_col
                        });

                        if is_editing {
                            let text = &mut state.editing_cell.as_mut().unwrap().2;
                            let response = ui.add(
                                egui::TextEdit::singleline(text)
                                    .desired_width(ui.available_width())
                                    .frame(true),
                            );
                            response.request_focus();

                            if ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                                let val = text.clone();
                                commit_edit = Some((actual_row, actual_col, val));
                            }
                            if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
                                state.editing_cell = None;
                            }
                        } else {
                            let cell_text_owned = state.get_cell(data, actual_row, actual_col).to_string();

                            let mut rt = egui::RichText::new(&cell_text_owned);
                            if is_highlight {
                                rt = rt.background_color(egui::Color32::from_rgb(255, 255, 80));
                            }

                            // Selection background
                            if is_sel {
                                let rect = ui.available_rect_before_wrap();
                                ui.painter().rect_filled(
                                    rect,
                                    0.0,
                                    egui::Color32::from_rgba_premultiplied(60, 120, 220, 80),
                                );
                            }

                            // Conditional highlight background
                            if let Some(hl_color) = state.highlight_color_for(&cell_text_owned, actual_col) {
                                let rect = ui.available_rect_before_wrap();
                                ui.painter().rect_filled(rect, 0.0, hl_color);
                            }

                            // Cursor outline
                            if is_cursor {
                                let rect = ui.available_rect_before_wrap();
                                ui.painter().rect_stroke(
                                    rect,
                                    0.0,
                                    egui::Stroke::new(2.0, egui::Color32::from_rgb(0, 120, 215)),
                                    egui::StrokeKind::Inside,
                                );
                            }

                            let cell_rect = ui.available_rect_before_wrap();

                            ui.add(egui::Label::new(rt));

                            let cell_response = ui.interact(
                                cell_rect,
                                ui.id().with(("cell", display_row, display_col)),
                                egui::Sense::click(),
                            );

                            if cell_response.clicked() {
                                if let Some((_, edit_col_display, ref text)) = state.editing_cell {
                                    let edit_display_row = state.editing_cell.as_ref().unwrap().0;
                                    let edit_actual = state.actual_row(edit_display_row);
                                    let edit_actual_col = vis_cols_snapshot.get(edit_col_display).copied().unwrap_or(edit_col_display);
                                    commit_edit = Some((edit_actual, edit_actual_col, text.clone()));
                                }

                                let pos = CellPos {
                                    row: display_row,
                                    col: display_col,
                                };
                                if ui.input(|i| i.modifiers.shift) {
                                    state.selection_end = Some(pos);
                                } else {
                                    state.selection_anchor = Some(pos);
                                    state.selection_end = Some(pos);
                                }
                                state.cursor = Some(pos);
                            }

                            if cell_response.double_clicked() {
                                start_edit = Some((display_row, display_col, cell_text_owned.clone()));
                            }

                            // Cell right-click menu for highlight
                            cell_response.context_menu(|ui| {
                                if ui.button("Highlight this value (column)").clicked() {
                                    let color_idx = state.highlight_rules.len() % HIGHLIGHT_COLORS.len();
                                    add_highlight_rule = Some(HighlightRule {
                                        col: Some(actual_col),
                                        op: HighlightOp::Equals,
                                        value: cell_text_owned.clone(),
                                        color: HIGHLIGHT_COLORS[color_idx],
                                    });
                                    ui.close_menu();
                                }
                                if ui.button("Highlight this value (all columns)").clicked() {
                                    let color_idx = state.highlight_rules.len() % HIGHLIGHT_COLORS.len();
                                    add_highlight_rule = Some(HighlightRule {
                                        col: None,
                                        op: HighlightOp::Equals,
                                        value: cell_text_owned.clone(),
                                        color: HIGHLIGHT_COLORS[color_idx],
                                    });
                                    ui.close_menu();
                                }
                            });
                        }
                    });
                }
            });
        });

    // Apply collected intents
    if let Some((actual, col, val)) = commit_edit {
        state.commit_edit(actual, col, val);
        state.editing_cell = None;
    }
    if let Some((dr, dc, text)) = start_edit {
        state.editing_cell = Some((dr, dc, text));
    }
    if let Some(col) = toggle_freeze_col {
        state.frozen_cols = col;
    }
    if let Some(col) = compute_stats_col {
        state.compute_column_stats(data, col);
    }
    if let Some(ac) = hide_col {
        state.hide_column(ac);
    }
    if let Some(di) = move_col_left {
        state.move_column_left(di);
    }
    if let Some(di) = move_col_right {
        state.move_column_right(di);
    }
    if let Some(rule) = add_highlight_rule {
        state.highlight_rules.push(rule);
    }

    }); // end horizontal ScrollArea

    // Stats popup window
    if state.stats_popup.is_some() {
        let stats = state.stats_popup.clone().unwrap();
        let col_name = headers.get(stats.col).map_or("?", |s| s.as_str()).to_string();
        let mut open = true;

        egui::Window::new(format!("Statistics: {}", col_name))
            .open(&mut open)
            .resizable(false)
            .show(ui.ctx(), |ui| {
                egui::Grid::new("stats_grid").striped(true).show(ui, |ui| {
                    ui.label("Count:");
                    ui.label(format!("{}", stats.count));
                    ui.end_row();

                    ui.label("Non-empty:");
                    ui.label(format!("{}", stats.non_empty));
                    ui.end_row();

                    ui.label("Empty:");
                    ui.label(format!("{}", stats.count - stats.non_empty));
                    ui.end_row();

                    ui.label("Unique:");
                    ui.label(format!("{}", stats.unique));
                    ui.end_row();

                    if stats.numeric_count > 0 {
                        ui.label("");
                        ui.label("");
                        ui.end_row();

                        ui.label("Numeric count:");
                        ui.label(format!("{}", stats.numeric_count));
                        ui.end_row();

                        ui.label("Sum:");
                        ui.label(format!("{}", stats.sum));
                        ui.end_row();

                        if let Some(avg) = stats.avg() {
                            ui.label("Average:");
                            ui.label(format!("{:.4}", avg));
                            ui.end_row();
                        }
                        if let Some(min) = stats.min {
                            ui.label("Min:");
                            ui.label(format!("{}", min));
                            ui.end_row();
                        }
                        if let Some(max) = stats.max {
                            ui.label("Max:");
                            ui.label(format!("{}", max));
                            ui.end_row();
                        }
                    }
                });

                if !stats.top_values.is_empty() {
                    ui.separator();
                    ui.label(egui::RichText::new("Top Values").strong());
                    ui.add_space(4.0);
                    let max_count = stats.top_values.first().map_or(1, |v| v.1).max(1);
                    let bar_max_width = 150.0_f32;

                    for (val, count) in &stats.top_values {
                        ui.horizontal(|ui| {
                            let label = if val.len() > 30 {
                                format!("{}...", &val[..27])
                            } else {
                                val.clone()
                            };
                            ui.label(
                                egui::RichText::new(format!("{:>6}  ", count)).monospace(),
                            );
                            let bar_width = (*count as f32 / max_count as f32) * bar_max_width;
                            let (rect, _) = ui.allocate_exact_size(
                                egui::vec2(bar_max_width, 14.0),
                                egui::Sense::hover(),
                            );
                            let bar_rect = egui::Rect::from_min_size(
                                rect.min,
                                egui::vec2(bar_width, 14.0),
                            );
                            ui.painter().rect_filled(
                                bar_rect,
                                2.0,
                                egui::Color32::from_rgb(0, 120, 215),
                            );
                            ui.label(&label);
                        });
                    }
                }
            });

        if !open {
            state.stats_popup = None;
        }
    }

    // Highlight rules dialog
    if state.show_highlight_dialog {
        let mut open = true;
        let mut new_rule: Option<HighlightRule> = None;
        let mut remove_rule: Option<usize> = None;

        egui::Window::new("Conditional Highlight")
            .open(&mut open)
            .resizable(false)
            .show(ui.ctx(), |ui| {
                ui.label(egui::RichText::new("Add Rule").strong());
                ui.horizontal(|ui| {
                    ui.label("Column:");
                    egui::ComboBox::from_id_salt("hl_col")
                        .selected_text(if state.highlight_dialog_col == usize::MAX {
                            "All".to_string()
                        } else {
                            headers.get(state.highlight_dialog_col).cloned().unwrap_or_else(|| "?".to_string())
                        })
                        .show_ui(ui, |ui| {
                            ui.selectable_value(&mut state.highlight_dialog_col, usize::MAX, "All").clicked();
                            for (i, h) in headers.iter().enumerate() {
                                ui.selectable_value(&mut state.highlight_dialog_col, i, h).clicked();
                            }
                        });
                });
                ui.horizontal(|ui| {
                    ui.label("Operator:");
                    egui::ComboBox::from_id_salt("hl_op")
                        .selected_text(state.highlight_dialog_op.label())
                        .show_ui(ui, |ui| {
                            for op in HighlightOp::ALL {
                                if ui.selectable_value(&mut state.highlight_dialog_op, op, op.label()).clicked() {}
                            }
                        });
                });
                ui.horizontal(|ui| {
                    ui.label("Value:");
                    ui.text_edit_singleline(&mut state.highlight_dialog_value);
                });
                ui.horizontal(|ui| {
                    ui.label("Color:");
                    for (i, &color) in HIGHLIGHT_COLORS.iter().enumerate() {
                        let size = egui::vec2(20.0, 20.0);
                        let (rect, resp) = ui.allocate_exact_size(size, egui::Sense::click());
                        ui.painter().rect_filled(rect, 2.0, color);
                        if state.highlight_dialog_color_idx == i {
                            ui.painter().rect_stroke(
                                rect,
                                2.0,
                                egui::Stroke::new(2.0, egui::Color32::WHITE),
                                egui::StrokeKind::Inside,
                            );
                        }
                        if resp.clicked() {
                            state.highlight_dialog_color_idx = i;
                        }
                    }
                });
                if ui.button("Add").clicked() && !state.highlight_dialog_value.is_empty() {
                    new_rule = Some(HighlightRule {
                        col: if state.highlight_dialog_col == usize::MAX { None } else { Some(state.highlight_dialog_col) },
                        op: state.highlight_dialog_op,
                        value: state.highlight_dialog_value.clone(),
                        color: HIGHLIGHT_COLORS[state.highlight_dialog_color_idx % HIGHLIGHT_COLORS.len()],
                    });
                    state.highlight_dialog_value.clear();
                }

                ui.separator();
                ui.label(egui::RichText::new("Active Rules").strong());
                let rules_len = state.highlight_rules.len();
                for i in 0..rules_len {
                    let rule = &state.highlight_rules[i];
                    ui.horizontal(|ui| {
                        let (rect, _) = ui.allocate_exact_size(egui::vec2(14.0, 14.0), egui::Sense::hover());
                        ui.painter().rect_filled(rect, 2.0, rule.color);
                        let col_label = match rule.col {
                            None => "All".to_string(),
                            Some(c) => headers.get(c).cloned().unwrap_or_else(|| "?".to_string()),
                        };
                        ui.label(format!("{} {} \"{}\"", col_label, rule.op.label(), rule.value));
                        if ui.small_button("x").clicked() {
                            remove_rule = Some(i);
                        }
                    });
                }
            });

        if let Some(rule) = new_rule {
            state.highlight_rules.push(rule);
        }
        if let Some(idx) = remove_rule {
            state.highlight_rules.remove(idx);
        }
        if !open {
            state.show_highlight_dialog = false;
        }
    }
}
