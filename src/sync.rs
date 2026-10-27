use crate::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::BufRead};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootState {
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub generation: i64,
    #[serde(default)]
    pub schema_version: i32,
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub hash: String,
    #[serde(rename = "type", default)]
    pub entry_type: String,
    pub id: String,
    pub subfiles: i64,
    pub size: i64,
}
impl SchemaEntry {
    pub fn new(hash: impl Into<String>, id: impl Into<String>, subfiles: i64, size: i64) -> Self {
        Self {
            hash: hash.into(),
            id: id.into(),
            entry_type: "0".into(),
            subfiles,
            size,
        }
    }
    pub fn format(&self) -> String {
        let t = if self.entry_type.is_empty() {
            String::new()
        } else {
            format!("{}:", self.entry_type)
        };
        format!(
            "{}:{t}{}:{}:{}\n",
            self.hash, self.id, self.subfiles, self.size
        )
    }
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub hash: String,
    pub entries: Vec<SchemaEntry>,
}
impl Manifest {
    pub fn find(&self, id: &str) -> Option<&SchemaEntry> {
        self.entries.iter().find(|e| e.id == id)
    }
    pub fn find_suffix(&self, suffix: &str) -> Option<&SchemaEntry> {
        self.entries.iter().find(|e| e.id.ends_with(suffix))
    }
    pub fn filter_prefix(&self, prefix: &str) -> Vec<&SchemaEntry> {
        self.entries
            .iter()
            .filter(|e| e.id.starts_with(prefix))
            .collect()
    }
    pub fn write(&self, mut writer: impl std::io::Write) -> Result<()> {
        for entry in &self.entries {
            writer.write_all(entry.format().as_bytes())?;
        }
        Ok(())
    }
}
pub fn parse_manifest(text: &str, hash: impl Into<String>) -> Result<Manifest> {
    read_manifest(text.as_bytes(), hash)
}
pub fn read_manifest(reader: impl BufRead, hash: impl Into<String>) -> Result<Manifest> {
    let mut entries = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| Error::domain(ErrorKind::InvalidSchema, e.to_string()))?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || (index == 0 && !line.contains(':')) {
            continue;
        }
        let parts: Vec<_> = line.split(':').collect();
        let invalid = || {
            Error::domain(
                ErrorKind::InvalidSchema,
                format!("line {}: {line}", index + 1),
            )
        };
        if parts.len() < 4 {
            return Err(invalid());
        }
        let legacy = parts.len() == 4;
        entries.push(SchemaEntry {
            hash: parts[0].into(),
            entry_type: if legacy {
                String::new()
            } else {
                parts[1].into()
            },
            id: parts[if legacy { 1 } else { 2 }].into(),
            subfiles: parts[parts.len() - 2].parse().map_err(|_| invalid())?,
            size: parts[parts.len() - 1].parse().map_err(|_| invalid())?,
        });
    }
    Ok(Manifest {
        hash: hash.into(),
        entries,
    })
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RedirValue {
    pub value: i64,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DeletedValue {
    pub value: i64,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CPage {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redir: Option<RedirValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted: Option<DeletedValue>,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OriginalPages {
    #[serde(default)]
    pub pages: Vec<CPage>,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CPages {
    #[serde(default)]
    pub pages: Vec<CPage>,
    #[serde(default)]
    pub original: OriginalPages,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DocumentContent {
    pub c_pages: CPages,
    pub file_type: String,
    pub format_version: i64,
    pub page_count: i64,
    pub pages: Vec<String>,
    pub orientation: String,
    pub cover_page_number: i64,
    pub original_page_count: i64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}
