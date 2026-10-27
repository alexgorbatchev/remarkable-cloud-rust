use crate::{
    Client, Error, ErrorKind, Manifest, Result, RootState, SchemaEntry, client::insert_header,
};
use base64::Engine;
use reqwest::{Method, header::HeaderMap};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileUpdate {
    pub name: String,
    pub data: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct UpdateDocumentOptions {
    pub id: String,
    pub expected_hash: String,
    pub expected_root: RootState,
    pub files: Vec<FileUpdate>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpdateState {
    #[default]
    Staged,
    CommitUnknown,
    Committed,
    Verified,
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateResult {
    pub state: UpdateState,
    pub uploaded: Vec<String>,
    pub root_hash: String,
}
#[derive(Clone, Debug)]
pub struct CreateDocumentOptions {
    pub id: String,
    pub expected_root: RootState,
    pub files: Vec<FileUpdate>,
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateResult {
    pub state: UpdateState,
    pub id: String,
    pub document_hash: String,
    pub root_hash: String,
    pub generation: i64,
    pub uploaded: Vec<String>,
}
#[derive(Debug)]
pub struct MutationFailure<T> {
    pub progress: T,
    pub error: Error,
}
impl<T: std::fmt::Debug> std::fmt::Display for MutationFailure<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl<T: std::fmt::Debug> std::error::Error for MutationFailure<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
pub type MutationResult<T> = std::result::Result<T, Box<MutationFailure<T>>>;

#[derive(Clone, Debug)]
pub struct EncodedManifest {
    pub data: Vec<u8>,
    pub hash: String,
    pub size: i64,
}
pub fn content_hash(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}
pub fn encode_cloud_manifest(entries: &[SchemaEntry], root: bool) -> Result<EncodedManifest> {
    let mut entries: Vec<_> = entries.iter().filter(|e| e.id != ".").collect();
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    let mut size = 0i64;
    for entry in &entries {
        if entry.id.is_empty() || entry.id.contains(['\r', '\n', ':']) || entry.size < 0 {
            return Err(Error::domain(
                ErrorKind::InvalidSchema,
                format!("invalid schema entry {:?}", entry.id),
            ));
        }
        size = size
            .checked_add(entry.size)
            .ok_or_else(|| Error::domain(ErrorKind::InvalidSchema, "manifest size overflow"))?;
    }
    let mut data = if root {
        format!("4\n0:.:{}:{size}\n", entries.len())
    } else {
        "3\n".into()
    };
    let mut hasher = Sha256::new();
    let mut previous: Option<&str> = None;
    for entry in entries {
        if previous == Some(&entry.id) {
            return Err(Error::domain(
                ErrorKind::InvalidSchema,
                format!("duplicate schema entry {}", entry.id),
            ));
        }
        previous = Some(&entry.id);
        let hash = hex::decode(&entry.hash).map_err(|_| {
            Error::domain(
                ErrorKind::InvalidSchema,
                format!("invalid hash for {}", entry.id),
            )
        })?;
        if hash.len() != 32 {
            return Err(Error::domain(
                ErrorKind::InvalidSchema,
                format!("invalid hash for {}", entry.id),
            ));
        }
        hasher.update(hash);
        let entry_type = if root || entry.entry_type.is_empty() {
            "0"
        } else {
            &entry.entry_type
        };
        if entry_type.contains(['\r', '\n', ':']) {
            return Err(Error::domain(
                ErrorKind::InvalidSchema,
                format!("invalid entry type for {}", entry.id),
            ));
        }
        data.push_str(&format!(
            "{}:{entry_type}:{}:{}:{}\n",
            entry.hash, entry.id, entry.subfiles, entry.size
        ));
    }
    let hash = if root {
        content_hash(data.as_bytes())
    } else {
        hex::encode(hasher.finalize())
    };
    Ok(EncodedManifest {
        data: data.into_bytes(),
        hash,
        size,
    })
}
pub fn validate_expected_root(root: &RootState) -> Result<()> {
    if root.generation < 0 || hex::decode(&root.hash).map_or(true, |v| v.len() != 32) {
        return Err(Error::message(
            "valid expected root hash and generation are required",
        ));
    }
    Ok(())
}
pub fn validate_document_files(id: &str, files: &[FileUpdate]) -> Result<()> {
    if files.is_empty() {
        return Err(Error::message("document files are required"));
    }
    let mut seen = HashSet::new();
    for file in files {
        if !file.name.starts_with(&format!("{id}/")) && !file.name.starts_with(&format!("{id}.")) {
            return Err(Error::message(format!(
                "file {:?} does not belong to {id}",
                file.name
            )));
        }
        if file.name.contains(['\r', '\n', ':']) || !seen.insert(&file.name) {
            return Err(Error::message(format!(
                "invalid or duplicate file {:?}",
                file.name
            )));
        }
    }
    Ok(())
}
struct Prepared {
    generation: i64,
    document: EncodedManifest,
    root: EncodedManifest,
}
impl Client {
    pub async fn update_document_files(
        &self,
        id: &str,
        expected_hash: &str,
        files: &[FileUpdate],
    ) -> MutationResult<UpdateResult> {
        self.update_document_files_with_progress(id, expected_hash, files, &mut |_| Ok(()))
            .await
    }
    /// Observe commit boundaries even when the calling future is cancelled.
    /// Rejecting staged or commit-unknown progress prevents the root commit.
    pub async fn update_document_files_with_progress(
        &self,
        id: &str,
        expected_hash: &str,
        files: &[FileUpdate],
        progress: &mut dyn FnMut(UpdateResult) -> Result<()>,
    ) -> MutationResult<UpdateResult> {
        let mut result = UpdateResult::default();
        let operation = async {
            validate_update(id, expected_hash, files)?;
            let root = self.get_root_state().await?;
            let prepared = self.prepare_update(id, expected_hash, files, &root).await?;
            self.apply_update(id, files, prepared, &mut result, progress)
                .await
        }
        .await;
        outcome(result, operation)
    }
    pub async fn update_document_files_at_root(
        &self,
        options: &UpdateDocumentOptions,
    ) -> MutationResult<UpdateResult> {
        self.update_document_files_at_root_with_progress(options, &mut |_| Ok(()))
            .await
    }
    /// Update from a guarded root while reporting the same boundaries as document creation.
    pub async fn update_document_files_at_root_with_progress(
        &self,
        options: &UpdateDocumentOptions,
        progress: &mut dyn FnMut(UpdateResult) -> Result<()>,
    ) -> MutationResult<UpdateResult> {
        let mut result = UpdateResult::default();
        let operation = async {
            validate_expected_root(&options.expected_root)?;
            let root = self.get_root_state().await?;
            guard_root(&root, &options.expected_root)?;
            let prepared = self
                .prepare_update(&options.id, &options.expected_hash, &options.files, &root)
                .await?;
            self.apply_update(&options.id, &options.files, prepared, &mut result, progress)
                .await
        }
        .await;
        outcome(result, operation)
    }
    async fn prepare_update(
        &self,
        id: &str,
        expected_hash: &str,
        files: &[FileUpdate],
        root: &RootState,
    ) -> Result<Prepared> {
        validate_update(id, expected_hash, files)?;
        let mut root_manifest = self.manifest_fresh(&root.hash, "root.docSchema").await?;
        let entry = root_manifest
            .find(id)
            .filter(|e| e.hash == expected_hash)
            .ok_or_else(|| Error::message(format!("destination {id} changed since preflight")))?;
        let mut manifest = self
            .manifest_fresh(&entry.hash, &format!("{id}.docSchema"))
            .await?;
        validate_document_files(id, files)?;
        for file in files {
            if let Some(entry) = manifest.entries.iter_mut().find(|e| e.id == file.name) {
                entry.hash = content_hash(&file.data);
                entry.size = file.data.len() as i64;
            } else {
                manifest.entries.push(file_entry(file));
            }
        }
        let document = encode_cloud_manifest(&manifest.entries, false)?;
        let entry = root_manifest
            .entries
            .iter_mut()
            .find(|e| e.id == id)
            .ok_or_else(|| Error::message("destination disappeared from manifest"))?;
        entry.hash = document.hash.clone();
        entry.size = document.size;
        entry.subfiles = manifest.entries.iter().filter(|e| e.id != ".").count() as i64;
        Ok(Prepared {
            generation: root.generation,
            document,
            root: encode_cloud_manifest(&root_manifest.entries, true)?,
        })
    }
    async fn apply_update(
        &self,
        id: &str,
        files: &[FileUpdate],
        prepared: Prepared,
        result: &mut UpdateResult,
        progress: &mut dyn FnMut(UpdateResult) -> Result<()>,
    ) -> Result<()> {
        result.root_hash = prepared.root.hash.clone();
        progress(result.clone())?;
        self.stage(id, files, &prepared, &mut result.uploaded)
            .await?;
        result.root_hash = prepared.root.hash.clone();
        result.state = UpdateState::CommitUnknown;
        if let Err(error) = progress(result.clone()) {
            result.state = UpdateState::Staged;
            return Err(error);
        }
        if let Err(error) = self
            .commit_root(&prepared.root.hash, prepared.generation)
            .await
        {
            if error.kind() == Some(ErrorKind::GenerationConflict) {
                result.state = UpdateState::Staged;
            }
            return Err(error);
        }
        result.state = UpdateState::Committed;
        progress(result.clone())?;
        self.verify_document_files(id, &prepared.document.hash, files)
            .await?;
        result.state = UpdateState::Verified;
        progress(result.clone())
    }
    pub async fn create_document(
        &self,
        options: &CreateDocumentOptions,
    ) -> MutationResult<CreateResult> {
        self.create_document_with_progress(options, &mut |_| Ok(()))
            .await
    }
    /// A callback error before the commit prevents it; errors afterward retain committed progress.
    pub async fn create_document_with_progress(
        &self,
        options: &CreateDocumentOptions,
        progress: &mut dyn FnMut(CreateResult) -> Result<()>,
    ) -> MutationResult<CreateResult> {
        let mut result = CreateResult {
            id: options.id.clone(),
            generation: options.expected_root.generation,
            ..CreateResult::default()
        };
        let operation = async {
            let prepared = self.prepare_creation(options).await?;
            result.document_hash = prepared.document.hash.clone();
            result.root_hash = prepared.root.hash.clone();
            progress(result.clone())?;
            self.stage(&options.id, &options.files, &prepared, &mut result.uploaded)
                .await?;
            result.state = UpdateState::CommitUnknown;
            if let Err(error) = progress(result.clone()) {
                result.state = UpdateState::Staged;
                return Err(error);
            }
            if let Err(error) = self
                .commit_root(&prepared.root.hash, prepared.generation)
                .await
            {
                if error.kind() == Some(ErrorKind::GenerationConflict) {
                    result.state = UpdateState::Staged;
                }
                return Err(error);
            }
            result.state = UpdateState::Committed;
            progress(result.clone())?;
            self.verify_document_files(&options.id, &prepared.document.hash, &options.files)
                .await?;
            result.state = UpdateState::Verified;
            progress(result.clone())
        }
        .await;
        outcome(result, operation)
    }
    async fn prepare_creation(&self, options: &CreateDocumentOptions) -> Result<Prepared> {
        let id = &options.id;
        if id.is_empty() || id.contains(['\r', '\n', ':', '/', '\\', '.']) {
            return Err(Error::message(format!("invalid document ID {id:?}")));
        }
        validate_expected_root(&options.expected_root)?;
        validate_document_files(id, &options.files)?;
        let root = self.get_root_state().await?;
        guard_root(&root, &options.expected_root)?;
        let mut manifest = self.manifest_fresh(&root.hash, "root.docSchema").await?;
        if manifest.find(id).is_some() {
            return Err(Error::message(format!("destination {id} already exists")));
        }
        let entries: Vec<_> = options.files.iter().map(file_entry).collect();
        let document = encode_cloud_manifest(&entries, false)?;
        manifest.entries.push(SchemaEntry::new(
            &document.hash,
            id,
            entries.len() as i64,
            document.size,
        ));
        Ok(Prepared {
            generation: root.generation,
            document,
            root: encode_cloud_manifest(&manifest.entries, true)?,
        })
    }
    async fn stage(
        &self,
        id: &str,
        files: &[FileUpdate],
        prepared: &Prepared,
        uploaded: &mut Vec<String>,
    ) -> Result<()> {
        for file in files {
            self.put_verified_blob(&content_hash(&file.data), &file.name, &file.data)
                .await?;
            uploaded.push(file.name.clone());
        }
        self.put_verified_blob(
            &prepared.document.hash,
            &format!("{id}.docSchema"),
            &prepared.document.data,
        )
        .await?;
        self.put_verified_blob(&prepared.root.hash, "root.docSchema", &prepared.root.data)
            .await
    }
    async fn put_verified_blob(&self, hash: &str, name: &str, bytes: &[u8]) -> Result<()> {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, "rm-filename", name)?;
        insert_header(
            &mut headers,
            "x-goog-hash",
            &format!(
                "crc32c={}",
                base64::engine::general_purpose::STANDARD
                    .encode(crc32c::crc32c(bytes).to_be_bytes())
            ),
        )?;
        insert_header(
            &mut headers,
            "content-type",
            if name == "root.docSchema" {
                "text/plain; charset=UTF-8"
            } else {
                "application/octet-stream"
            },
        )?;
        let response = self
            .request(
                Method::PUT,
                &format!("/sync/v3/files/{hash}"),
                headers,
                bytes.into(),
            )
            .await?;
        if !matches!(response.status, 200..=202) {
            return Err(response.status_error(format!("upload {name}"), None));
        }
        if self.get_blob_fresh(hash, name).await? != bytes {
            return Err(Error::message(format!(
                "uploaded bytes mismatch for {name}"
            )));
        }
        Ok(())
    }
    async fn commit_root(&self, hash: &str, generation: i64) -> Result<()> {
        #[derive(Serialize)]
        struct Commit<'a> {
            hash: &'a str,
            generation: i64,
            broadcast: bool,
        }
        let bytes = serde_json::to_vec(&Commit {
            hash,
            generation,
            broadcast: true,
        })?;
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, "content-type", "application/json")?;
        insert_header(&mut headers, "rm-filename", "roothash")?;
        let response = self
            .request(Method::PUT, "/sync/v3/root", headers, bytes)
            .await?;
        if response.status != 200 {
            let kind =
                matches!(response.status, 409 | 412).then_some(ErrorKind::GenerationConflict);
            return Err(response.status_error("commit root", kind));
        }
        let result: RootState = serde_json::from_slice(&response.into_body()?)?;
        if result.hash != hash || result.generation <= generation {
            return Err(Error::message("unexpected root commit response"));
        }
        Ok(())
    }
    async fn verify_document_files(
        &self,
        id: &str,
        document_hash: &str,
        files: &[FileUpdate],
    ) -> Result<()> {
        let root = self.get_root_state().await?;
        let manifest = self.manifest_fresh(&root.hash, "root.docSchema").await?;
        if !manifest.find(id).is_some_and(|e| e.hash == document_hash) {
            return Err(Error::message("committed destination association changed"));
        }
        let manifest: Manifest = self
            .manifest_fresh(document_hash, &format!("{id}.docSchema"))
            .await?;
        for file in files {
            let hash = content_hash(&file.data);
            if !manifest.find(&file.name).is_some_and(|e| e.hash == hash) {
                return Err(Error::message(format!(
                    "verifying association for {}",
                    file.name
                )));
            }
            if self.get_blob_fresh(&hash, &file.name).await? != file.data {
                return Err(Error::message(format!(
                    "verifying bytes for {}: mismatch",
                    file.name
                )));
            }
        }
        Ok(())
    }
}
fn validate_update(id: &str, hash: &str, files: &[FileUpdate]) -> Result<()> {
    if id.is_empty() || hash.is_empty() || files.is_empty() {
        return Err(Error::message(
            "document ID, expected hash, and files are required",
        ));
    }
    Ok(())
}
fn guard_root(actual: &RootState, expected: &RootState) -> Result<()> {
    if actual.hash != expected.hash || actual.generation != expected.generation {
        return Err(Error::domain(
            ErrorKind::GenerationConflict,
            "root changed since preflight",
        ));
    }
    Ok(())
}
fn file_entry(file: &FileUpdate) -> SchemaEntry {
    SchemaEntry::new(
        content_hash(&file.data),
        &file.name,
        0,
        file.data.len() as i64,
    )
}
fn outcome<T>(progress: T, result: Result<()>) -> MutationResult<T> {
    match result {
        Ok(()) => Ok(progress),
        Err(error) => Err(Box::new(MutationFailure { progress, error })),
    }
}
