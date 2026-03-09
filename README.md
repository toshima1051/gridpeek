<p align="center">
  <img src="assets/icon.svg" width="128" height="128" alt="GridPeek icon">
</p>

<h1 align="center">GridPeek</h1>

<p align="center">
  Ultra-lightweight CSV / Excel / TSV viewer for Windows, Linux, and macOS.<br>
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="License">
  <img src="https://img.shields.io/github/v/release/toshima1051/gridpeek?include_prereleases" alt="Version">
</p>

## Features

- **Blazing-fast loading** -- memory-mapped I/O with parallel line indexing (rayon + memchr)
- **Multiple file formats** -- CSV, TSV, TXT (delimited), XLSX, XLS, XLSB, XLSM, ODS
- **Tabbed interface** -- open multiple files, drag-and-drop to add tabs, Ctrl+Tab to switch
- **Column management** -- hide, reorder, and auto-width columns
- **Search & Replace** -- incremental search with regex support, works across CSV and Excel files
- **Conditional highlighting** -- color-code cells by rules (equals, contains, greater/less than)
- **Filtering** -- per-column text filters with real-time results
- **Column statistics** -- min, max, sum, average, count for numeric columns
- **Duplicate detection** -- find and highlight duplicate rows
- **Cell editing** -- in-place edit with undo support
- **Export** -- export to CSV, TSV, or XLSX with column selection, row range, sampling, and row limit
- **Windows "Open with"** -- register/unregister from Settings menu (no installer needed)
- **Tiny binary** -- under 5 MB with all features

## Download

Pre-built binaries are available on the [Releases](https://github.com/toshima1051/gridpeek/releases) page:

| Platform | File |
|----------|------|
| Windows x64 | `gridpeek-x86_64-pc-windows-msvc.zip` |
| Linux x64 | `gridpeek-x86_64-unknown-linux-gnu.tar.gz` |
| macOS x64 | `gridpeek-x86_64-apple-darwin.tar.gz` |
| macOS ARM | `gridpeek-aarch64-apple-darwin.tar.gz` |

## Usage

```
gridpeek [file]
```

Or drag and drop files onto the window.

### Windows "Open with" Registration

Go to **Settings > Register "Open with"** to add GridPeek to the right-click "Open with" menu for supported file types. No administrator privileges required.

### Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| Ctrl+O | Open file |
| Ctrl+S | Save |
| Ctrl+W | Close tab |
| Ctrl+Tab | Next tab |
| Ctrl+Shift+Tab | Previous tab |
| Ctrl+F | Find |
| Ctrl+H | Find & Replace |
| Ctrl+Z | Undo |
| Ctrl+C | Copy selection |
| Ctrl+Shift+W | Auto column width |
| Ctrl+= / Ctrl+- | Zoom in / out |

## Build from Source

Requires [Rust](https://rustup.rs/) 1.85+ (edition 2024).

```bash
cargo build --release
```

The binary will be at `target/release/gridpeek` (or `gridpeek.exe` on Windows).

## License

[MIT](LICENSE)

---

# GridPeek (日本語)

超軽量 CSV / Excel / TSV ビューア。Windows、Linux、macOS 対応。

## 特徴

- **高速読み込み** -- メモリマップドI/O + 並列行インデックス構築 (rayon + memchr)
- **多フォーマット対応** -- CSV, TSV, TXT, XLSX, XLS, XLSB, XLSM, ODS
- **タブインターフェース** -- 複数ファイルを同時に開く、ドラッグ&ドロップ対応
- **列管理** -- 列の非表示・並べ替え・自動幅調整
- **検索・置換** -- インクリメンタルサーチ、正規表現対応、CSV/Excel 両対応
- **条件付きハイライト** -- セルを条件でカラーコード表示（一致・含む・大小比較）
- **フィルタ** -- 列ごとのテキストフィルタ
- **列統計** -- 数値列の最小・最大・合計・平均・件数
- **重複検出** -- 重複行の検出とハイライト
- **セル編集** -- その場で編集、アンドゥ対応
- **エクスポート** -- CSV/TSV/XLSX へエクスポート（列選択・行範囲・サンプリング・行数制限）
- **Windows「プログラムから開く」** -- 設定メニューから登録（インストーラ不要）
- **超軽量バイナリ** -- 全機能込みで 5MB 以下

## ダウンロード

[Releases](https://github.com/toshima1051/gridpeek/releases) ページからビルド済みバイナリをダウンロードできます。

## 使い方

```
gridpeek [ファイル]
```

またはウィンドウにファイルをドラッグ&ドロップしてください。

## ライセンス

[MIT](LICENSE)

