//! Result files: the committed raw evidence behind every published number.
//!
//! Schema (`bench/results/<name>.json`):
//!
//! ```json
//! {
//!   "schema": 1,
//!   "name": "lob_replay_throughput",
//!   "created_unix_ms": 1783465097000,
//!   "host": { "cpu": "...", "cores": 18, "mem_gb": 64, "os": "...", "rustc": "..." },
//!   "config": { ... benchmark-specific ... },
//!   "metrics": { ... benchmark-specific numbers/percentiles ... },
//!   "notes": "..."
//! }
//! ```

use std::path::{Path, PathBuf};

/// Host fingerprint recorded into every result file.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HostInfo {
    /// CPU brand string.
    pub cpu: String,
    /// Logical cores.
    pub cores: u32,
    /// Physical memory, GiB.
    pub mem_gb: u64,
    /// OS name + version.
    pub os: String,
    /// rustc version used for the build.
    pub rustc: String,
}

fn cmd_line(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

impl HostInfo {
    /// Detect the current host (macOS sysctl/sw_vers with Linux fallbacks;
    /// unknown fields degrade to "unknown", never fail).
    pub fn detect() -> HostInfo {
        let cpu = cmd_line("sysctl", &["-n", "machdep.cpu.brand_string"])
            .or_else(|| {
                // Linux fallback
                std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| {
                    s.lines()
                        .find(|l| l.starts_with("model name"))
                        .and_then(|l| l.split(':').nth(1))
                        .map(|v| v.trim().to_string())
                })
            })
            .unwrap_or_else(|| "unknown".into());
        let cores = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(0);
        let mem_gb = cmd_line("sysctl", &["-n", "hw.memsize"])
            .and_then(|s| s.parse::<u64>().ok())
            .map(|b| b / (1024 * 1024 * 1024))
            .or_else(|| {
                std::fs::read_to_string("/proc/meminfo")
                    .ok()
                    .and_then(|s| {
                        s.lines().find(|l| l.starts_with("MemTotal")).and_then(|l| {
                            l.split_whitespace()
                                .nth(1)
                                .and_then(|kb| kb.parse::<u64>().ok())
                        })
                    })
                    .map(|kb| kb / (1024 * 1024))
            })
            .unwrap_or(0);
        let os = cmd_line("sw_vers", &["-productVersion"])
            .map(|v| format!("macOS {v}"))
            .or_else(|| {
                std::fs::read_to_string("/etc/os-release")
                    .ok()
                    .and_then(|s| {
                        s.lines().find(|l| l.starts_with("PRETTY_NAME=")).map(|l| {
                            l.trim_start_matches("PRETTY_NAME=")
                                .trim_matches('"')
                                .to_string()
                        })
                    })
            })
            .unwrap_or_else(|| "unknown".into());
        let rustc = cmd_line("rustc", &["--version"])
            .or_else(|| {
                let home = std::env::var("HOME").ok()?;
                cmd_line(&format!("{home}/.cargo/bin/rustc"), &["--version"])
            })
            .unwrap_or_else(|| "unknown".into());
        HostInfo {
            cpu,
            cores,
            mem_gb,
            os,
            rustc,
        }
    }
}

/// One result file's content.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResultFile {
    /// Schema version (this file layout).
    pub schema: u32,
    /// Benchmark name (also the file stem).
    pub name: String,
    /// Wall-clock creation time, ms since epoch.
    pub created_unix_ms: u64,
    /// Host fingerprint.
    pub host: HostInfo,
    /// Benchmark-specific configuration (symbol set, sizes, modes...).
    pub config: serde_json::Value,
    /// Benchmark-specific metrics (throughputs, [`crate::Percentiles`]...).
    pub metrics: serde_json::Value,
    /// Free-form methodology notes for this run.
    pub notes: String,
}

impl ResultFile {
    /// New result with the current host and timestamp.
    pub fn new(
        name: &str,
        config: serde_json::Value,
        metrics: serde_json::Value,
        notes: &str,
    ) -> ResultFile {
        ResultFile {
            schema: 1,
            name: name.to_string(),
            created_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0),
            host: HostInfo::detect(),
            config,
            metrics,
            notes: notes.to_string(),
        }
    }
}

/// Write a result file atomically (tmp + rename) as `<dir>/<name>.json`.
/// Refuses to overwrite an existing file unless `overwrite` is set —
/// results are evidence; clobbering them silently is how numbers get faked.
pub fn write_result(dir: &Path, r: &ResultFile, overwrite: bool) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}.json", r.name));
    if !overwrite && path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("{} exists; pass overwrite explicitly", path.display()),
        ));
    }
    let tmp = dir.join(format!(".{}.json.tmp", r.name));
    let json = serde_json::to_string_pretty(r).expect("result serializes");
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, &path)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_detect_has_content() {
        let h = HostInfo::detect();
        assert!(!h.cpu.is_empty());
        assert!(h.cores > 0);
    }

    #[test]
    fn write_and_refuse_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let r = ResultFile::new(
            "unit_test_result",
            serde_json::json!({"k": 1}),
            serde_json::json!({"v": 2}),
            "test",
        );
        let p = write_result(dir.path(), &r, false).unwrap();
        let back: ResultFile = serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        assert_eq!(back.name, "unit_test_result");
        assert_eq!(back.schema, 1);
        assert_eq!(back.metrics["v"], 2);
        // second write without overwrite fails
        assert!(write_result(dir.path(), &r, false).is_err());
        // with overwrite succeeds
        write_result(dir.path(), &r, true).unwrap();
    }
}
