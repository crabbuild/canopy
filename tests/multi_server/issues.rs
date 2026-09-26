use super::tokens::{finish_upload, paused_upload};
use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";

async fn status(request: reqwest::RequestBuilder, expected: StatusCode) -> Result {
    let response = request.send().await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}

async fn value(request: reqwest::RequestBuilder) -> Result<Value> {
    Ok(request.send().await?.error_for_status()?.json().await?)
}

fn issue(repository: &Value, id: &str, title: &str) -> Value {
    json!({"repository_id": repository, "id": id, "title": title, "body": "Details with Unicode: 树 🌲\n<script>raw text</script>"})
}

#[tokio::test(flavor = "multi_thread")]
async fn issues_and_comments_are_versioned_authorized_and_recoverable() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    create_repository(address, "team").await?;
    create_repository(address, "other").await?;
    let client = Client::new();
    let base = format!("http://{address}");
    let repo = format!("{base}/api/repositories/team");
    let api = format!("{repo}/issues");
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let mut credentials = Vec::new();
    for (index, (account, scope, role)) in [
        ("alice", "write", Some("read")),
        ("bob", "write", Some("read")),
        ("maintainer", "write", Some("write")),
        ("outsider", "write", None),
        ("viewer", "read", Some("read")),
    ]
    .into_iter()
    .enumerate()
    {
        let token = format!("cnp_{index:064x}");
        status(
            client
                .post(format!("{base}/api/accounts"))
                .bearer_auth(OWNER)
                .json(&json!({"name":account, "token":token, "scope":scope})),
            StatusCode::OK,
        )
        .await?;
        if let Some(role) = role {
            status(
                client
                    .put(format!("{repo}/collaborators/{account}"))
                    .bearer_auth(OWNER)
                    .json(&json!({"role":role})),
                StatusCode::OK,
            )
            .await?;
        }
        credentials.push(token);
    }
    let [alice, bob, maintainer, outsider, viewer] = credentials.as_slice() else {
        return Err("missing accounts".into());
    };
    status(client.get(&api), StatusCode::UNAUTHORIZED).await?;
    status(
        client.get(&api).bearer_auth(outsider),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let first_id = uuid::Uuid::new_v4().to_string();
    let first = issue(&repository, &first_id, "First issue");
    status(
        client.post(&api).bearer_auth(viewer).json(&first),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.post(&api).bearer_auth(outsider).json(&first),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let (one, retry) = tokio::join!(
        value(client.post(&api).bearer_auth(alice).json(&first)),
        value(client.post(&api).bearer_auth(alice).json(&first)),
    );
    assert_eq!(one?, json!({"number":1}));
    assert_eq!(retry?, json!({"number":1}));
    status(
        client.post(&api).bearer_auth(bob).json(&first),
        StatusCode::CONFLICT,
    )
    .await?;
    let mut conflict = first.clone();
    conflict["title"] = json!("Changed original");
    status(
        client.post(&api).bearer_auth(alice).json(&conflict),
        StatusCode::CONFLICT,
    )
    .await?;
    let detail = value(client.get(format!("{api}/1")).bearer_auth(viewer)).await?;
    assert_eq!(detail["issue"]["author"], "alice");
    assert_eq!(detail["issue"]["body"], first["body"]);
    assert_eq!(detail["issue"]["version"], 1);
    assert_eq!(detail["issue"]["state"], "open");
    assert!(
        detail["issue"]["created_at_ms"]
            .as_i64()
            .ok_or("missing time")?
            > 0
    );
    status(
        client.get(format!("{api}/999")).bearer_auth(OWNER),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client.get(format!("{api}/999/comments")).bearer_auth(OWNER),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let mut edit = json!({"repository_id":repository, "expected_version":1, "title":"Edited title", "body":"Updated description", "state":"closed"});
    status(
        client.put(format!("{api}/1")).bearer_auth(bob).json(&edit),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client
            .put(format!("{api}/1"))
            .bearer_auth(maintainer)
            .json(&edit),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client
            .put(format!("{api}/1"))
            .bearer_auth(alice)
            .json(&edit),
        StatusCode::CONFLICT,
    )
    .await?;
    edit["expected_version"] = json!(2);
    edit["state"] = json!("open");
    status(
        client
            .put(format!("{api}/1"))
            .bearer_auth(alice)
            .json(&edit),
        StatusCode::NO_CONTENT,
    )
    .await?;
    edit["expected_version"] = json!(3);
    edit["state"] = json!("closed");
    let mut competing = edit.clone();
    competing["body"] = json!("Concurrent content");
    let (a, b) = tokio::join!(
        client
            .put(format!("{api}/1"))
            .bearer_auth(alice)
            .json(&edit)
            .send(),
        client
            .put(format!("{api}/1"))
            .bearer_auth(maintainer)
            .json(&competing)
            .send(),
    );
    let mut codes = [a?.status().as_u16(), b?.status().as_u16()];
    codes.sort();
    assert_eq!(codes, [204, 409]);
    // Creation retry retains its original binding and never undoes an edit.
    assert_eq!(
        value(client.post(&api).bearer_auth(alice).json(&first)).await?,
        json!({"number":1})
    );
    let edited = value(client.get(format!("{api}/1")).bearer_auth(OWNER)).await?;
    assert_eq!(edited["issue"]["version"], 4);
    assert_eq!(edited["issue"]["state"], "closed");
    assert_eq!(edited["issue"]["title"], "Edited title");

    let comments_api = format!("{api}/1/comments");
    let comment_id = uuid::Uuid::new_v4().to_string();
    let comment =
        json!({"repository_id":repository, "id":comment_id, "body":"A comment on a closed issue"});
    assert_eq!(
        value(client.post(&comments_api).bearer_auth(alice).json(&comment)).await?,
        json!({"number":1})
    );
    let comment_edit =
        json!({"repository_id":repository, "expected_version":1, "body":"Clarified comment"});
    status(
        client
            .put(format!("{comments_api}/1"))
            .bearer_auth(bob)
            .json(&comment_edit),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client
            .put(format!("{comments_api}/1"))
            .bearer_auth(alice)
            .json(&comment_edit),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client
            .put(format!("{comments_api}/1"))
            .bearer_auth(maintainer)
            .json(&comment_edit),
        StatusCode::CONFLICT,
    )
    .await?;
    assert_eq!(
        value(client.post(&comments_api).bearer_auth(alice).json(&comment)).await?,
        json!({"number":1})
    );
    let mut wrong_comment = comment.clone();
    wrong_comment["body"] = json!("Different original comment");
    status(
        client
            .post(&comments_api)
            .bearer_auth(alice)
            .json(&wrong_comment),
        StatusCode::CONFLICT,
    )
    .await?;
    assert_eq!(
        value(client.post(&api).bearer_auth(bob).json(&issue(
            &repository,
            &uuid::Uuid::new_v4().to_string(),
            "Second issue"
        )))
        .await?,
        json!({"number":2})
    );
    status(
        client
            .post(format!("{api}/2/comments"))
            .bearer_auth(alice)
            .json(&comment),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        client
            .put(format!("{api}/2/comments/1"))
            .bearer_auth(OWNER)
            .json(&comment_edit),
        StatusCode::NOT_FOUND,
    )
    .await?;
    for number in 2..=17 {
        let comment = json!({"repository_id":repository, "id":uuid::Uuid::new_v4().to_string(), "body":"x".repeat(canopy_server::issues::ISSUE_BODY_LIMIT)});
        assert_eq!(
            value(client.post(&comments_api).bearer_auth(bob).json(&comment)).await?,
            json!({"number":number})
        );
    }
    let comment_page = value(client.get(&comments_api).bearer_auth(viewer)).await?;
    assert_eq!(
        comment_page["comments"]
            .as_array()
            .ok_or("missing comments")?
            .len(),
        16
    );
    assert_eq!(comment_page["next_after"], 16);
    assert_eq!(comment_page["comments"][0]["body"], "Clarified comment");
    assert_eq!(comment_page["comments"][0]["version"], 2);
    let comment_end = value(
        client
            .get(&comments_api)
            .query(&[("after", 16)])
            .bearer_auth(viewer),
    )
    .await?;
    assert_eq!(
        comment_end["comments"]
            .as_array()
            .ok_or("missing comments")?
            .len(),
        1
    );
    assert_eq!(comment_end["comments"][0]["number"], 17);
    assert!(comment_end["next_after"].is_null());
    for number in 3..=33 {
        assert_eq!(
            value(client.post(&api).bearer_auth(OWNER).json(&issue(
                &repository,
                &uuid::Uuid::new_v4().to_string(),
                &format!("Issue {number}")
            )))
            .await?,
            json!({"number":number})
        );
    }
    let page = value(client.get(&api).bearer_auth(viewer)).await?;
    assert_eq!(page["issues"].as_array().ok_or("missing issues")?.len(), 32);
    assert_eq!(page["next_after"], 32);
    assert!(
        page["issues"]
            .as_array()
            .ok_or("missing issues")?
            .iter()
            .all(|row| row.get("body").is_none() && row.get("creation_digest").is_none())
    );
    let end = value(client.get(&api).query(&[("after", 32)]).bearer_auth(viewer)).await?;
    assert_eq!(end["issues"][0]["number"], 33);
    assert!(end["next_after"].is_null());
    let closed = value(
        client
            .get(&api)
            .query(&[("state", "closed")])
            .bearer_auth(viewer),
    )
    .await?;
    assert_eq!(
        closed["issues"].as_array().ok_or("missing closed")?.len(),
        1
    );
    assert_eq!(closed["issues"][0]["number"], 1);
    let open = value(
        client
            .get(&api)
            .query(&[("state", "open")])
            .bearer_auth(viewer),
    )
    .await?;
    assert_eq!(open["issues"].as_array().ok_or("missing open")?.len(), 32);
    assert_eq!(open["next_after"], 33);
    let terminal = value(
        client
            .get(&api)
            .query(&[("state", "open"), ("after", "33")])
            .bearer_auth(viewer),
    )
    .await?;
    assert_eq!(terminal["issues"], json!([]));
    assert!(terminal["next_after"].is_null());

    for (field, invalid) in [
        ("title", json!(" ")),
        ("title", json!("a\nb")),
        ("title", json!("x".repeat(257))),
        ("body", json!("x".repeat(16385))),
        ("body", json!("bad\0body")),
        ("id", json!(uuid::Uuid::nil().to_string())),
    ] {
        let mut bad = first.clone();
        bad[field] = invalid;
        status(
            client.post(&api).bearer_auth(OWNER).json(&bad),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    let mut stale_repository = first.clone();
    stale_repository["repository_id"] = json!(uuid::Uuid::new_v4().to_string());
    status(
        client.post(&api).bearer_auth(alice).json(&stale_repository),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        client
            .post(&api)
            .bearer_auth(OWNER)
            .body("x".repeat(128 * 1024 + 1)),
        StatusCode::PAYLOAD_TOO_LARGE,
    )
    .await?;
    for path in [&api, &comments_api] {
        status(
            client.get(path).query(&[("after", -1)]).bearer_auth(OWNER),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    let other_api = format!("{base}/api/repositories/other/issues");
    assert_eq!(
        value(client.get(&other_api).bearer_auth(OWNER)).await?["issues"],
        json!([])
    );
    status(
        client.get(format!("{other_api}/1")).bearer_auth(OWNER),
        StatusCode::NOT_FOUND,
    )
    .await?;

    // HTTP admission precedes the body. ACL revocation must still stop both
    // mutations when their buffered bodies finally reach the Cell transaction.
    let delayed = issue(
        &repository,
        &uuid::Uuid::new_v4().to_string(),
        "Must not publish",
    );
    let delayed_body = serde_json::to_vec(&delayed)?;
    let delayed_comment = serde_json::to_vec(
        &json!({"repository_id":repository, "id":uuid::Uuid::new_v4().to_string(), "body":"Must not publish"}),
    )?;
    let issue_upload = paused_upload(
        address,
        "/api/repositories/team/issues",
        alice,
        delayed_body.len(),
    )
    .await?;
    let comment_upload = paused_upload(
        address,
        "/api/repositories/team/issues/1/comments",
        alice,
        delayed_comment.len(),
    )
    .await?;
    status(
        client
            .delete(format!("{repo}/collaborators/alice"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    finish_upload(issue_upload, &delayed_body, 404).await?;
    finish_upload(comment_upload, &delayed_comment, 404).await?;
    for path in [&api, &format!("{api}/1"), &comments_api] {
        status(client.get(path).bearer_auth(alice), StatusCode::NOT_FOUND).await?;
    }
    status(
        client.post(&api).bearer_auth(alice).json(&first),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client
            .put(format!("{api}/1"))
            .bearer_auth(alice)
            .json(&edit),
        StatusCode::NOT_FOUND,
    )
    .await?;
    assert!(
        value(client.get(&api).query(&[("after", 33)]).bearer_auth(OWNER)).await?["issues"]
            .as_array()
            .ok_or("missing issues")?
            .is_empty()
    );
    let before = value(client.get(format!("{api}/1")).bearer_auth(OWNER)).await?;
    let before_comments = value(client.get(&comments_api).bearer_auth(OWNER)).await?;
    status(
        client
            .patch(&repo)
            .bearer_auth(OWNER)
            .json(&json!({"name":"renamed", "repository_id":repository})),
        StatusCode::OK,
    )
    .await?;
    status(client.get(&api).bearer_auth(OWNER), StatusCode::NOT_FOUND).await?;
    server.shutdown().await?;
    let restored_address = available_address().await?;
    let restored = CanopyServer::start(
        config(restored_address, workspace.path().join("restored")),
        Arc::clone(&store),
    )
    .await?;
    let restored_api = format!("http://{restored_address}/api/repositories/renamed/issues");
    assert_eq!(
        value(client.get(format!("{restored_api}/1")).bearer_auth(viewer)).await?,
        before
    );
    assert_eq!(
        value(
            client
                .get(format!("{restored_api}/1/comments"))
                .bearer_auth(viewer)
        )
        .await?,
        before_comments
    );
    status(
        client.get(&restored_api).bearer_auth(alice),
        StatusCode::NOT_FOUND,
    )
    .await?;
    assert_eq!(
        value(client.post(&restored_api).bearer_auth(OWNER).json(&delayed)).await?,
        json!({"number":34})
    );
    // A retry after owner restore retains the same creation number and content.
    assert_eq!(
        value(client.post(&restored_api).bearer_auth(OWNER).json(&delayed)).await?,
        json!({"number":34})
    );
    edit["expected_version"] = json!(4);
    edit["state"] = json!("open");
    status(
        client
            .put(format!("{restored_api}/1"))
            .bearer_auth(maintainer)
            .json(&edit),
        StatusCode::NO_CONTENT,
    )
    .await?;
    assert_eq!(
        value(client.get(format!("{restored_api}/1")).bearer_auth(OWNER)).await?["issue"]["version"],
        5
    );
    restored.shutdown().await?;
    Ok(())
}
