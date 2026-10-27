use crate::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, Write},
    path::{Path, PathBuf},
};

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Config {
    #[serde(rename = "devicetoken", default)]
    pub device_token: String,
    #[serde(rename = "usertoken", default)]
    pub user_token: String,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("device_token", &"[redacted]")
            .field("user_token", &"[redacted]")
            .finish()
    }
}
impl Config {
    pub fn parse(text: &str) -> Result<Self> {
        Self::read(text.as_bytes())
    }
    pub fn read(reader: impl BufRead) -> Result<Self> {
        let mut config = Self::default();
        for line in reader.lines() {
            let line = line.map_err(|e| Error::domain(ErrorKind::InvalidConfig, e.to_string()))?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = line.split_once(':') {
                match key.trim().to_ascii_lowercase().as_str() {
                    "devicetoken" => config.device_token = value.trim().into(),
                    "usertoken" => config.user_token = value.trim().into(),
                    _ => (),
                }
            }
        }
        Ok(config)
    }
    pub fn read_file(path: &Path) -> Result<Self> {
        match std::fs::File::open(path) {
            Ok(file) => Self::read(std::io::BufReader::new(file)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(Error::domain(
                ErrorKind::ConfigNotFound,
                path.display().to_string(),
            )),
            Err(e) => Err(e.into()),
        }
    }
    pub fn write(&self, mut writer: impl Write) -> Result<()> {
        write!(
            writer,
            "devicetoken: {}\nusertoken: {}\n",
            self.device_token, self.user_token
        )?;
        Ok(())
    }
    pub fn write_file(&self, path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(parent)?;
        let mut file = tempfile::NamedTempFile::new_in(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        self.write(&mut file)?;
        file.as_file().sync_all()?;
        file.persist(path).map_err(|e| Error::Io(e.error))?;
        Ok(())
    }
}

pub fn config_candidates() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(base) = std::env::var_os("RMAPI_CONFIG").filter(|s| !s.is_empty()) {
        let base = PathBuf::from(base);
        if base.is_file() {
            paths.push(base);
        } else {
            paths.extend(
                [".rmapi", ".rmapi.conf", "rmapi/.rmapi", "rmapi/rmapi.conf"].map(|s| base.join(s)),
            );
        }
    }
    let home = dirs::home_dir();
    if let Some(home) = &home {
        paths.extend([home.join(".rmapi"), home.join(".rmapi.conf")]);
    }
    let xdg = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(|| home.as_ref().map(|h| h.join(".config")));
    if let Some(base) = xdg {
        paths.extend([base.join("rmapi/rmapi.conf"), base.join("rmapi/.rmapi")]);
    }
    if let Some(home) = home {
        paths.extend([
            home.join("Library/Application Support/rmapi/rmapi.conf"),
            home.join("Library/Application Support/rmapi/.rmapi"),
        ]);
    }
    paths
}
pub fn resolve_config_path(override_path: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = override_path.filter(|p| !p.as_os_str().is_empty()) {
        return Ok(path.into());
    }
    if let Some(path) = config_candidates().into_iter().find(|p| p.is_file()) {
        return Ok(path);
    }
    dirs::home_dir()
        .map(|p| p.join(".rmapi"))
        .ok_or_else(|| Error::domain(ErrorKind::ConfigNotFound, "home directory unavailable"))
}
pub fn load_config(override_path: Option<&Path>) -> Result<(Config, PathBuf)> {
    let path = resolve_config_path(override_path)?;
    Ok((Config::read_file(&path)?, path))
}
