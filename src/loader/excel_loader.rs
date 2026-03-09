use calamine::{open_workbook_auto, Data, Reader};
use std::path::Path;

use super::DataSource;

pub struct ExcelLoader {
    sheets: Vec<SheetData>,
    current_sheet: usize,
}

struct SheetData {
    name: String,
    headers: Vec<String>,
    /// All cell data stored as owned strings
    rows: Vec<Vec<String>>,
}

impl ExcelLoader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let mut workbook = open_workbook_auto(path.as_ref())?;
        let sheet_names: Vec<String> = workbook.sheet_names().to_vec();

        let mut sheets = Vec::new();

        for name in &sheet_names {
            if let Ok(range) = workbook.worksheet_range(name) {
                let mut rows_iter = range.rows();

                // First row as headers
                let headers: Vec<String> = if let Some(first_row) = rows_iter.next() {
                    first_row.iter().map(cell_to_string).collect()
                } else {
                    vec![]
                };

                // Remaining rows as data
                let rows: Vec<Vec<String>> = rows_iter
                    .map(|row| row.iter().map(cell_to_string).collect())
                    .collect();

                sheets.push(SheetData {
                    name: name.clone(),
                    headers,
                    rows,
                });
            }
        }

        if sheets.is_empty() {
            return Err("No readable sheets found".into());
        }

        Ok(ExcelLoader {
            sheets,
            current_sheet: 0,
        })
    }

    fn current(&self) -> &SheetData {
        &self.sheets[self.current_sheet]
    }
}

impl DataSource for ExcelLoader {
    fn headers(&self) -> &[String] {
        &self.current().headers
    }

    fn row_count(&self) -> usize {
        self.current().rows.len()
    }

    fn cell(&self, row: usize, col: usize) -> &str {
        self.current()
            .rows
            .get(row)
            .and_then(|r| r.get(col))
            .map(|s| s.as_str())
            .unwrap_or("")
    }


    fn sheet_count(&self) -> usize {
        self.sheets.len()
    }

    fn sheet_names(&self) -> Vec<String> {
        self.sheets.iter().map(|s| s.name.clone()).collect()
    }

    fn switch_sheet(&mut self, index: usize) {
        if index < self.sheets.len() {
            self.current_sheet = index;
        }
    }
}

fn cell_to_string(cell: &Data) -> String {
    match cell {
        Data::Empty => String::new(),
        Data::String(s) => s.clone(),
        Data::Int(i) => i.to_string(),
        Data::Float(f) => {
            // Avoid unnecessary decimal places
            if *f == (*f as i64) as f64 && f.abs() < 1e15 {
                format!("{}", *f as i64)
            } else {
                f.to_string()
            }
        }
        Data::Bool(b) => b.to_string(),
        Data::Error(e) => format!("{:?}", e),
        Data::DateTime(f) => {
            f.to_string()
        }
        _ => String::new(),
    }
}
