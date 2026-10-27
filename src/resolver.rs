use crate::{
    AmbiguousCandidate, AmbiguousNameError, Client, DocumentContent, Error, ErrorKind, Manifest,
    Result, SchemaEntry,
};
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

pub type ItemType = String;
pub const ITEM_TYPE_DOCUMENT: &str = "DocumentType";
pub const ITEM_TYPE_COLLECTION: &str = "CollectionType";

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ItemMetadata {
    pub visible_name: String,
    #[serde(rename = "type")]
    pub item_type: ItemType,
    pub parent: String,
    pub deleted: bool,
    pub last_modified: String,
    pub version: i64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Item {
    pub id: String,
    pub hash: String,
    pub metadata: ItemMetadata,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<SchemaEntry>,
    #[serde(skip)]
    client: Option<Client>,
}
impl Item {
    pub fn new(
        id: impl Into<String>,
        hash: impl Into<String>,
        metadata: ItemMetadata,
        entries: Vec<SchemaEntry>,
    ) -> Self {
        Self {
            id: id.into(),
            hash: hash.into(),
            metadata,
            entries,
            client: None,
        }
    }
    pub fn client(&self) -> Option<&Client> {
        self.client.as_ref()
    }
    pub fn bind_client(&mut self, client: Client) {
        self.client = Some(client);
    }
    pub fn is_document(&self) -> bool {
        self.metadata.item_type == ITEM_TYPE_DOCUMENT
    }
    pub fn is_collection(&self) -> bool {
        self.metadata.item_type == ITEM_TYPE_COLLECTION
    }
    pub fn find_entry_suffix(&self, suffix: &str) -> Option<&SchemaEntry> {
        self.entries.iter().find(|e| e.id.ends_with(suffix))
    }
    pub async fn get_manifest(&self) -> Result<Manifest> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::message("no client bound to item"))?;
        let hash = if !self.hash.is_empty() {
            &self.hash
        } else {
            &self
                .find_entry_suffix(".docSchema")
                .ok_or_else(|| {
                    Error::domain(
                        ErrorKind::ItemNotFound,
                        format!("missing schema hash for item {}", self.id),
                    )
                })?
                .hash
        };
        client
            .get_manifest(hash, &format!("{}.docSchema", self.id))
            .await
    }
    pub async fn get_content(&self) -> Result<DocumentContent> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::message("no client bound to item"))?;
        let mut entry = self.find_entry_suffix(".content").cloned();
        if entry.is_none()
            && let Ok(manifest) = self.get_manifest().await
        {
            entry = manifest.find_suffix(".content").cloned();
        }
        let entry = entry.ok_or_else(|| {
            Error::domain(
                ErrorKind::ItemNotFound,
                format!("content entry not found for item {}", self.id),
            )
        })?;
        client.get_document_content(&entry.hash, &entry.id).await
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct ListOptions {
    pub include_deleted: bool,
}
impl Client {
    pub async fn list_items(&self, options: ListOptions) -> Result<Vec<Item>> {
        let root = self.get_root_state().await?;
        let manifest = self.get_manifest(&root.hash, "root.docSchema").await?;
        let entries: Vec<_> = manifest
            .entries
            .into_iter()
            .filter(|e| e.id != ".")
            .collect();
        let mut items = Vec::new();
        if entries.iter().any(|e| e.id.contains('.')) {
            let mut groups: BTreeMap<String, Vec<SchemaEntry>> = BTreeMap::new();
            for entry in entries {
                let id = entry
                    .id
                    .split(['.', '/'])
                    .next()
                    .unwrap_or_default()
                    .to_owned();
                groups.entry(id).or_default().push(entry);
            }
            for (id, entries) in groups {
                let mut metadata =
                    if let Some(entry) = entries.iter().find(|e| e.id.ends_with(".metadata")) {
                        self.get_content::<ItemMetadata>(&entry.hash, &format!("{id}.metadata"))
                            .await?
                    } else {
                        ItemMetadata::default()
                    };
                if metadata.visible_name.is_empty() {
                    metadata.visible_name = id.clone();
                }
                if metadata.item_type.is_empty() {
                    metadata.item_type = ITEM_TYPE_DOCUMENT.into();
                }
                if !options.include_deleted && (metadata.deleted || metadata.parent == "trash") {
                    continue;
                }
                let hash = entries
                    .iter()
                    .find(|e| e.id.ends_with(".docSchema") || e.id == id)
                    .or_else(|| entries.first())
                    .map(|e| e.hash.clone())
                    .unwrap_or_default();
                let mut item = Item::new(id, hash, metadata, entries);
                item.bind_client(self.clone());
                items.push(item);
            }
        } else {
            let results: Vec<_> = stream::iter(entries)
                .map(|entry| async move { self.item_from_entry(entry).await })
                .buffer_unordered(16)
                .collect()
                .await;
            let mut errors = Vec::new();
            for result in results {
                match result {
                    Ok(item)
                        if options.include_deleted
                            || (!item.metadata.deleted && item.metadata.parent != "trash") =>
                    {
                        items.push(item)
                    }
                    Ok(_) => (),
                    Err(error) => errors.push(error),
                }
            }
            if !errors.is_empty() {
                return Err(Error::Multiple(errors));
            }
            items.sort_by(|a, b| a.id.cmp(&b.id));
        }
        Ok(items)
    }
    async fn item_from_entry(&self, entry: SchemaEntry) -> Result<Item> {
        let manifest = self
            .get_manifest(&entry.hash, &format!("{}.docSchema", entry.id))
            .await?;
        let mut metadata = if let Some(entry) = manifest.find_suffix(".metadata") {
            self.get_content::<ItemMetadata>(&entry.hash, &entry.id)
                .await?
        } else {
            ItemMetadata::default()
        };
        if metadata.visible_name.is_empty() {
            metadata.visible_name = entry.id.clone();
        }
        let mut item = Item::new(entry.id, entry.hash, metadata, manifest.entries);
        item.bind_client(self.clone());
        Ok(item)
    }
    pub async fn resolve_by_id(&self, id: &str) -> Result<Item> {
        let root = self.get_root_state().await?;
        let manifest = self.get_manifest(&root.hash, "root.docSchema").await?;
        if let Some(entry) = manifest.find(id) {
            return self.item_from_entry(entry.clone()).await;
        }
        self.list_items(ListOptions {
            include_deleted: true,
        })
        .await?
        .into_iter()
        .find(|i| i.id == id)
        .ok_or_else(|| Error::domain(ErrorKind::ItemNotFound, id))
    }
    pub async fn resolve_by_name(&self, name: &str) -> Result<Item> {
        resolve_items_by_name(&self.list_items(ListOptions::default()).await?, name)
    }
    pub async fn resolve_by_path(&self, path: &str) -> Result<Item> {
        resolve_items_by_path(&self.list_items(ListOptions::default()).await?, path)
    }
    pub async fn resolve(&self, query: &str) -> Result<Item> {
        let query = query.trim();
        if query.is_empty() {
            return Err(Error::domain(ErrorKind::ItemNotFound, "empty query"));
        }
        if !query.contains('/') {
            match self.resolve_by_id(query).await {
                Ok(item) => return Ok(item),
                Err(error) if error.kind() == Some(ErrorKind::ItemNotFound) => (),
                Err(error) => return Err(error),
            }
        } else {
            return self.resolve_by_path(query).await;
        }
        match self.resolve_by_name(query).await {
            Ok(item) => Ok(item),
            Err(error) if error.kind() == Some(ErrorKind::ItemNotFound) => {
                self.resolve_by_path(query).await
            }
            Err(error) => Err(error),
        }
    }
}
pub fn resolve_items_by_name(items: &[Item], name: &str) -> Result<Item> {
    let matches: Vec<_> = items
        .iter()
        .filter(|i| i.metadata.visible_name == name)
        .cloned()
        .collect();
    choose(items, matches, name, name)
}
pub fn resolve_items_by_path(items: &[Item], query: &str) -> Result<Item> {
    let path = query.trim().trim_matches('/');
    if path.is_empty() {
        return Err(Error::domain(ErrorKind::ItemNotFound, query));
    }
    let parts: Vec<_> = path.split('/').collect();
    let mut parent = String::new();
    let mut current = None;
    for (index, name) in parts.iter().enumerate() {
        let last = index + 1 == parts.len();
        let matches = items
            .iter()
            .filter(|i| {
                i.metadata.visible_name == *name
                    && i.metadata.parent == parent
                    && (last || i.is_collection())
            })
            .cloned()
            .collect();
        let item = choose(items, matches, query, name)?;
        parent = item.id.clone();
        current = Some(item);
    }
    current.ok_or_else(|| Error::domain(ErrorKind::ItemNotFound, query))
}
fn choose(items: &[Item], mut matches: Vec<Item>, query: &str, name: &str) -> Result<Item> {
    match matches.len() {
        0 => Err(Error::domain(ErrorKind::ItemNotFound, query)),
        1 => Ok(matches.remove(0)),
        _ => Err(AmbiguousNameError {
            query: query.into(),
            name: name.into(),
            candidates: ambiguous_candidates(items, &matches),
        }
        .into()),
    }
}
pub fn ambiguous_candidates(items: &[Item], matches: &[Item]) -> Vec<AmbiguousCandidate> {
    let by_id: HashMap<_, _> = items.iter().map(|i| (i.id.as_str(), i)).collect();
    let mut candidates: Vec<_> = matches
        .iter()
        .map(|item| {
            let mut parent = item.metadata.parent.as_str();
            let mut seen = HashSet::new();
            let mut names = Vec::new();
            let mut unreachable = false;
            while !parent.is_empty() {
                if !seen.insert(parent) {
                    unreachable = true;
                    break;
                }
                let Some(folder) = by_id.get(parent).filter(|i| {
                    i.is_collection() && !i.metadata.deleted && i.metadata.parent != "trash"
                }) else {
                    unreachable = true;
                    break;
                };
                names.push(folder.metadata.visible_name.clone());
                parent = folder.metadata.parent.as_str();
            }
            names.reverse();
            AmbiguousCandidate {
                item: item.clone(),
                folder_path: names.join("/"),
                unreachable,
            }
        })
        .collect();
    candidates.sort_by(|a, b| {
        (&a.unreachable, &a.folder_path, &a.item.id).cmp(&(
            &b.unreachable,
            &b.folder_path,
            &b.item.id,
        ))
    });
    candidates
}
