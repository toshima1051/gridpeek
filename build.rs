use std::path::Path;

fn render_svg(svg_data: &[u8], size: u32) -> Vec<u8> {
    let tree =
        resvg::usvg::Tree::from_data(svg_data, &resvg::usvg::Options::default()).unwrap();
    let mut pixmap = resvg::tiny_skia::Pixmap::new(size, size).unwrap();
    let scale = size as f32 / tree.size().width().max(tree.size().height());
    resvg::render(
        &tree,
        resvg::tiny_skia::Transform::from_scale(scale, scale),
        &mut pixmap.as_mut(),
    );
    pixmap.encode_png().unwrap()
}

fn write_ico(path: &Path, png_images: &[(u32, Vec<u8>)]) {
    let count = png_images.len() as u16;
    let mut data: Vec<u8> = Vec::new();

    // ICO header
    data.extend_from_slice(&0u16.to_le_bytes());
    data.extend_from_slice(&1u16.to_le_bytes());
    data.extend_from_slice(&count.to_le_bytes());

    let mut offset = 6u32 + 16 * count as u32;

    for (size, png) in png_images {
        let w = if *size >= 256 { 0u8 } else { *size as u8 };
        data.push(w);
        data.push(w);
        data.push(0);
        data.push(0);
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend_from_slice(&32u16.to_le_bytes());
        data.extend_from_slice(&(png.len() as u32).to_le_bytes());
        data.extend_from_slice(&offset.to_le_bytes());
        offset += png.len() as u32;
    }

    for (_, png) in png_images {
        data.extend_from_slice(png);
    }

    std::fs::write(path, data).unwrap();
}

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir);
    let svg_data = std::fs::read("assets/icon.svg").expect("assets/icon.svg not found");

    // Runtime icon (64x64 raw RGBA for eframe window icon)
    let tree =
        resvg::usvg::Tree::from_data(&svg_data, &resvg::usvg::Options::default()).unwrap();
    let mut pixmap = resvg::tiny_skia::Pixmap::new(64, 64).unwrap();
    let scale = 64.0 / tree.size().width().max(tree.size().height());
    resvg::render(
        &tree,
        resvg::tiny_skia::Transform::from_scale(scale, scale),
        &mut pixmap.as_mut(),
    );
    std::fs::write(out_path.join("icon_rgba.bin"), pixmap.data()).unwrap();

    // Windows .exe icon via embed-resource
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "windows" {
        let sizes = [16, 32, 48, 256];
        let images: Vec<(u32, Vec<u8>)> = sizes
            .iter()
            .map(|&s| (s, render_svg(&svg_data, s)))
            .collect();

        let ico_path = out_path.join("icon.ico");
        write_ico(&ico_path, &images);

        // Write .rc file
        let rc_path = out_path.join("icon.rc");
        let ico_path_str = ico_path.display().to_string().replace('\\', "/");
        std::fs::write(&rc_path, format!("1 ICON \"{}\"", ico_path_str)).unwrap();

        // Compile .rc to object using embed-resource
        let _ = embed_resource::compile(&rc_path, embed_resource::NONE);
    }

    println!("cargo::rerun-if-changed=assets/icon.svg");
}
