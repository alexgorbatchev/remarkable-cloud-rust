use remarkable_cloud::*;
use sha2::{Digest, Sha256};

#[test]
fn legacy_and_typed_manifests_and_unknown_content_are_preserved() {
    let manifest = parse_manifest("v3\na:doc:2:12\nb:0:doc.content:0:5\n", "root").unwrap();
    assert_eq!(manifest.entries[0].id, "doc");
    assert_eq!(manifest.entries[0].entry_type, "");
    assert_eq!(manifest.entries[1].entry_type, "0");
    assert!(parse_manifest("3\nbad\n", "root").is_err());
    let content: DocumentContent = serde_json::from_str(r#"{"cPages":{"pages":[{"id":"p","deleted":{"value":1}}]},"fileType":"pdf","tags":["work"]}"#).unwrap();
    assert_eq!(content.c_pages.pages[0].deleted.as_ref().unwrap().value, 1);
    assert_eq!(content.extra["tags"][0], "work");
    assert_eq!(serde_json::to_value(content).unwrap()["tags"][0], "work");
}

#[test]
fn cloud_hashes_use_distinct_root_and_document_algorithms() {
    let a = hex::encode(Sha256::digest(b"a"));
    let b = hex::encode(Sha256::digest(b"b"));
    let entries = vec![
        SchemaEntry::new(b.clone(), "z", 0, 1),
        SchemaEntry::new(a.clone(), "a", 0, 1),
    ];
    let doc = encode_cloud_manifest(&entries, false).unwrap();
    let root = encode_cloud_manifest(&entries, true).unwrap();
    let mut binary = hex::decode(a).unwrap();
    binary.extend(hex::decode(b).unwrap());
    assert_eq!(doc.hash, hex::encode(Sha256::digest(binary)));
    assert_eq!(root.hash, hex::encode(Sha256::digest(&root.data)));
    assert!(
        String::from_utf8(root.data)
            .unwrap()
            .starts_with("4\n0:.:2:2\n")
    );
    assert!(encode_cloud_manifest(&[entries[0].clone(), entries[0].clone()], false).is_err());
}

#[test]
fn config_round_trip_private_permissions_and_missing_classification() {
    let cfg = Config::parse("# comment\nDeviceToken: device\nUserToken: user\nignored\n").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested/config");
    cfg.write_file(&path).unwrap();
    assert_eq!(Config::read_file(&path).unwrap(), cfg);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert_eq!(
        Config::read_file(&dir.path().join("missing"))
            .unwrap_err()
            .kind(),
        Some(ErrorKind::ConfigNotFound)
    );
}

fn item(id: &str, name: &str, parent: &str, folder: bool) -> Item {
    Item::new(
        id,
        "hash",
        ItemMetadata {
            visible_name: name.into(),
            parent: parent.into(),
            item_type: if folder {
                ITEM_TYPE_COLLECTION
            } else {
                ITEM_TYPE_DOCUMENT
            }
            .into(),
            ..ItemMetadata::default()
        },
        vec![],
    )
}

#[test]
fn resolver_reports_sorted_ambiguity_and_unreachable_cycles() {
    let items = vec![
        item("f", "Work", "", true),
        item("d2", "Notes", "f", false),
        item("d1", "Notes", "", false),
        item("d3", "Notes", "absent", false),
        item("loop", "Loop", "loop", true),
        item("d4", "Notes", "loop", false),
    ];
    assert_eq!(
        resolve_items_by_path(&items, " /Work/Notes/ ").unwrap().id,
        "d2"
    );
    assert_eq!(resolve_items_by_name(&items, "Work").unwrap().id, "f");
    let Error::Ambiguous(error) = resolve_items_by_name(&items, "Notes").unwrap_err() else {
        panic!()
    };
    assert_eq!(
        error
            .candidates
            .iter()
            .map(|c| c.item.id.as_str())
            .collect::<Vec<_>>(),
        ["d1", "d2", "d3", "d4"]
    );
    assert!(error.candidates[2].unreachable);
    assert!(error.candidates[3].unreachable);
    assert!(error.to_string().contains("matches 4 items"));
    assert!(!error.to_string().contains("d1"));
    let folders = vec![item("a", "Same", "", true), item("b", "Same", "", true)];
    let error = resolve_items_by_path(&folders, "Same/child").unwrap_err();
    assert_eq!(error.kind(), Some(ErrorKind::AmbiguousName));
    assert!(error.to_string().contains("in \"Same/child\""));
    for path in ["", "/", "missing", "Work/missing", "Notes/child"] {
        assert!(resolve_items_by_path(&items, path).is_err());
    }
    assert!(format!("{:?}", items[0]).contains("Work"));
}

#[test]
fn schema_readers_and_writers_reject_invalid_dynamic_data() {
    use std::io::{self, BufRead, Read};
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("broken reader"))
        }
    }
    impl BufRead for Broken {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            Err(io::Error::other("broken buffer"))
        }
        fn consume(&mut self, _: usize) {}
    }
    assert_eq!(
        Config::read(Broken).unwrap_err().kind(),
        Some(ErrorKind::InvalidConfig)
    );
    assert_eq!(
        read_manifest(Broken, "hash").unwrap_err().kind(),
        Some(ErrorKind::InvalidSchema)
    );
    for line in ["3\na:id:bad:4", "3\na:0:id:0:bad", "# first\nversion"] {
        assert!(parse_manifest(line, "h").is_err());
    }
    let manifest = parse_manifest(
        "3\n# comment\n\na:document:0:2\nb:0:document.content:1:3\n",
        "h",
    )
    .unwrap();
    assert_eq!(manifest.filter_prefix("document").len(), 2);
    assert_eq!(manifest.find_suffix("content").unwrap().size, 3);
    let mut output = Vec::new();
    manifest.write(&mut output).unwrap();
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "a:document:0:2\nb:0:document.content:1:3\n"
    );
    assert!(manifest.write(io::Cursor::new(&mut [0u8; 1][..])).is_err());
    let good = SchemaEntry::new(content_hash(b"test"), "file", 0, 1);
    for (id, hash, entry_type, size) in [
        ("", good.hash.as_str(), "0", 0),
        ("bad:id", good.hash.as_str(), "0", 0),
        ("file", "bad", "0", 0),
        ("file", "00", "0", 0),
        ("file", good.hash.as_str(), "x:y", 0),
        ("file", good.hash.as_str(), "0", -1),
    ] {
        assert!(
            encode_cloud_manifest(
                &[SchemaEntry {
                    id: id.into(),
                    hash: hash.into(),
                    entry_type: entry_type.into(),
                    subfiles: 0,
                    size
                }],
                false
            )
            .is_err()
        );
    }
    assert!(
        encode_cloud_manifest(
            &[
                SchemaEntry {
                    size: i64::MAX,
                    ..good.clone()
                },
                SchemaEntry {
                    id: "other".into(),
                    ..good.clone()
                }
            ],
            false
        )
        .is_err()
    );
    assert!(validate_expected_root(&RootState::default()).is_err());
    assert!(validate_document_files("id", &[]).is_err());
    let file = FileUpdate {
        name: "id.file".into(),
        data: vec![],
    };
    assert!(validate_document_files("other", std::slice::from_ref(&file)).is_err());
    assert!(validate_document_files("id", &[file.clone(), file]).is_err());
    assert!(
        validate_document_files(
            "id",
            &[FileUpdate {
                name: "id.bad\n".into(),
                data: vec![]
            }]
        )
        .is_err()
    );
}

#[test]
fn config_environment_paths_are_checked_in_isolated_processes() {
    let executable = std::env::current_exe().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    std::fs::create_dir(&home).unwrap();
    for case in [
        "fallback",
        "rmapi-directory",
        "rmapi-file",
        "xdg",
        "home",
        "mac",
    ] {
        let base = dir.path().join(case);
        std::fs::create_dir_all(&base).unwrap();
        if case == "rmapi-file" {
            std::fs::write(base.join("config"), "usertoken: synthetic").unwrap();
        }
        if case == "xdg" {
            std::fs::create_dir(base.join("rmapi")).unwrap();
            std::fs::write(base.join("rmapi/rmapi.conf"), "usertoken: synthetic").unwrap();
        }
        if case == "home" {
            std::fs::write(base.join(".rmapi"), "usertoken: synthetic").unwrap();
        }
        if case == "mac" {
            std::fs::create_dir_all(base.join("Library/Application Support/rmapi")).unwrap();
            std::fs::write(
                base.join("Library/Application Support/rmapi/.rmapi"),
                "usertoken: synthetic",
            )
            .unwrap();
        }
        let mut command = std::process::Command::new(&executable);
        command
            .args(["--exact", "config_environment_child"])
            .env("CLOUD_CONFIG_CASE", case)
            .env("CLOUD_CONFIG_BASE", &base)
            .env("HOME", &home)
            .env_remove("RMAPI_CONFIG")
            .env_remove("XDG_CONFIG_HOME");
        match case {
            "rmapi-directory" => {
                command.env("RMAPI_CONFIG", &base);
            }
            "rmapi-file" => {
                command.env("RMAPI_CONFIG", base.join("config"));
            }
            "xdg" => {
                command.env("XDG_CONFIG_HOME", &base);
            }
            "home" | "mac" => {
                command.env("HOME", &base);
            }
            _ => (),
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}
#[test]
fn config_environment_child() {
    let Ok(case) = std::env::var("CLOUD_CONFIG_CASE") else {
        return;
    };
    let base = std::path::PathBuf::from(std::env::var_os("CLOUD_CONFIG_BASE").unwrap());
    let candidates = config_candidates();
    assert!(candidates.iter().any(|p| p.ends_with(".rmapi")));
    let expected = match case.as_str() {
        "rmapi-file" => base.join("config"),
        "xdg" => base.join("rmapi/rmapi.conf"),
        "home" => base.join(".rmapi"),
        "mac" => base.join("Library/Application Support/rmapi/.rmapi"),
        _ => std::path::PathBuf::from(std::env::var_os("HOME").unwrap()).join(".rmapi"),
    };
    assert_eq!(resolve_config_path(None).unwrap(), expected);
    assert_eq!(resolve_config_path(Some(&base)).unwrap(), base);
    if expected.exists() {
        assert_eq!(load_config(None).unwrap().1, expected);
    } else {
        assert_eq!(
            load_config(None).unwrap_err().kind(),
            Some(ErrorKind::ConfigNotFound)
        );
    }
    assert!(format!("{:?}", Config::default()).contains("redacted"));
}

#[test]
fn typed_error_display_keeps_context_and_sources() {
    use std::error::Error as _;
    for kind in [
        ErrorKind::ConfigNotFound,
        ErrorKind::InvalidConfig,
        ErrorKind::Unauthorized,
        ErrorKind::ItemNotFound,
        ErrorKind::AmbiguousName,
        ErrorKind::InvalidSchema,
        ErrorKind::DiscoveryFailed,
        ErrorKind::GenerationConflict,
    ] {
        let error = Error::Domain {
            kind,
            message: format!("context {kind}"),
        };
        assert!(error.to_string().contains("context"));
        assert_eq!(error.kind(), Some(kind));
    }
    let error = Error::Status(StatusError {
        op: "download".into(),
        status_code: 403,
        body: "server reason".into(),
        body_error: None,
        kind: Some(ErrorKind::Unauthorized),
    });
    assert!(error.to_string().contains("403: server reason"));
    assert_eq!(error.kind(), Some(ErrorKind::Unauthorized));
    let error = Error::Status(StatusError {
        op: "upload".into(),
        status_code: 500,
        body: String::new(),
        body_error: None,
        kind: None,
    });
    assert_eq!(error.to_string(), "upload failed with status 500");
    let failure = MutationFailure {
        progress: UpdateResult::default(),
        error,
    };
    assert!(failure.source().is_some());
    assert!(failure.to_string().contains("upload"));
}
