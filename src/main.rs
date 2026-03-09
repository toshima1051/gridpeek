#![windows_subsystem = "windows"]

mod app;
mod loader;
mod search;
mod viewer;

use std::path::PathBuf;

fn load_icon() -> eframe::egui::IconData {
    const ICON_RGBA: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/icon_rgba.bin"));
    eframe::egui::IconData {
        rgba: ICON_RGBA.to_vec(),
        width: 64,
        height: 64,
    }
}

fn main() -> eframe::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.get(1).map(|s| s.as_str()) == Some("--register") {
        register_file_associations();
        return Ok(());
    }
    if args.get(1).map(|s| s.as_str()) == Some("--unregister") {
        unregister_file_associations();
        return Ok(());
    }

    let initial_file: Option<PathBuf> = args.get(1).map(PathBuf::from);

    let mut viewport = eframe::egui::ViewportBuilder::default()
        .with_inner_size([1200.0, 800.0])
        .with_drag_and_drop(true);

    let icon = load_icon();
    viewport = viewport.with_icon(std::sync::Arc::new(icon));

    let options = eframe::NativeOptions {
        viewport,
        ..Default::default()
    };

    eframe::run_native(
        "GridPeek",
        options,
        Box::new(move |cc| Ok(Box::new(app::App::new(cc, initial_file)))),
    )
}

/// Register GridPeek in Windows "Open with" for CSV/Excel/TSV files
pub fn register_file_associations() {
    let exe_path = std::env::current_exe().unwrap_or_default();
    let exe_str = exe_path.to_string_lossy().replace('/', "\\");
    let command = format!("\"{}\" \"%1\"", exe_str);

    let app_key = r"HKCU\Software\Classes\Applications\gridpeek.exe";
    let extensions = [".csv", ".tsv", ".txt", ".xlsx", ".xls", ".xlsb", ".xlsm", ".ods"];

    let cmd_key = format!("{}\\shell\\open\\command", app_key);

    let mut ok = true;
    // Register app name
    if !run_reg(&["add", app_key, "/v", "FriendlyAppName", "/d", "GridPeek", "/f"]) { ok = false; }
    // Register open command
    if !run_reg(&["add", &cmd_key, "/ve", "/d", &command, "/f"]) { ok = false; }

    // SupportedTypes
    for ext in &extensions {
        let key = format!("{}\\SupportedTypes", app_key);
        if !run_reg(&["add", &key, "/v", ext, "/d", "", "/f"]) { ok = false; }
    }

    // OpenWithList per extension
    for ext in &extensions {
        let key = format!(r"HKCU\Software\Classes\{}\OpenWithList\gridpeek.exe", ext);
        if !run_reg(&["add", &key, "/ve", "/d", "", "/f"]) { ok = false; }
    }

    if ok {
        eprintln!("GridPeek registered for: {}", extensions.join(", "));
        eprintln!("Right-click a file > Open with > GridPeek");
    } else {
        eprintln!("Some registrations failed. Try running as administrator.");
    }
}

/// Remove GridPeek from Windows "Open with"
pub fn unregister_file_associations() {
    let app_key = r"HKCU\Software\Classes\Applications\gridpeek.exe";
    let extensions = [".csv", ".tsv", ".txt", ".xlsx", ".xls", ".xlsb", ".xlsm", ".ods"];

    run_reg(&["delete", app_key, "/f"]);
    for ext in &extensions {
        let key = format!(r"HKCU\Software\Classes\{}\OpenWithList\gridpeek.exe", ext);
        run_reg(&["delete", &key, "/f"]);
    }
    eprintln!("GridPeek file associations removed.");
}

fn run_reg(args: &[&str]) -> bool {
    #[cfg(target_os = "windows")]
    use std::os::windows::process::CommandExt;

    let mut cmd = std::process::Command::new("reg");
    cmd.args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    #[cfg(target_os = "windows")]
    cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW

    cmd.status().map(|s| s.success()).unwrap_or(false)
}
