use std::process::Command;
fn main() {
    // Unix timestamp of the build, injected as BUILD_STAMP so /hub status can
    // prove which binary a running session actually loaded.
    let stamp = Command::new("date")
        .arg("+%Y-%m-%dT%H:%M:%S")
        .output()
        .ok()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=BUILD_STAMP={stamp}");
    println!("cargo:rerun-if-changed=build.rs");
}
