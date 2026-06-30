//! S3-compatible presigned POST upload policy using AWS Signature Version 4.
//!
//! Builds a base64-encoded policy document and signs it with the HMAC-SHA256
//! signing key derived from the secret access key, date, region, and service.
//! Compatible with RustFS, MinIO, AWS S3, and any SigV4-compatible object store.

use crate::config::StorageConfig;
use crate::error::{Result, ShopError};
use hmac::KeyInit;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Fields returned to the client for constructing a browser POST form.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PresignedPost {
    /// The full URL to POST the upload to (endpoint + bucket).
    pub url: String,
    /// Form fields the client must include in the POST body.
    pub fields: BTreeMap<String, String>,
}

/// Build a presigned POST policy for uploading an object to S3-compatible
/// storage using SigV4.
pub fn build_presigned_post(
    cfg: &StorageConfig,
    object_key: &str,
    content_type: &str,
    max_size_bytes: u64,
    metadata: &BTreeMap<String, String>,
) -> Result<PresignedPost> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| ShopError::Internal(format!("system time error: {e}")))?;

    let amz_date = format_amz_date(now.as_secs());
    let date_stamp = &amz_date[..8]; // YYYYMMDD

    let expiry_secs = now.as_secs() + cfg.presigned_expiry_secs;
    let expiration = format_iso8601(expiry_secs);

    let credential = format!(
        "{}/{}/{}/s3/aws4_request",
        cfg.access_key, date_stamp, cfg.region
    );

    // Build the policy document
    let mut conditions: Vec<serde_json::Value> = vec![
        serde_json::json!({ "bucket": cfg.bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-algorithm": "AWS4-HMAC-SHA256" }),
        serde_json::json!({ "x-amz-credential": credential }),
        serde_json::json!({ "x-amz-date": amz_date }),
        serde_json::json!(["content-length-range", 0, max_size_bytes]),
    ];

    // Add content-type condition if provided
    if !content_type.is_empty() {
        conditions.push(serde_json::json!({ "Content-Type": content_type }));
    }

    // Add user metadata conditions
    for (k, v) in metadata {
        conditions.push(serde_json::json!({ k: v }));
    }

    let policy_doc = serde_json::json!({
        "expiration": expiration,
        "conditions": conditions,
    });

    let policy_json = serde_json::to_string(&policy_doc)?;
    let policy_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        policy_json.as_bytes(),
    );

    // Compute the signing key
    let date_key = hmac_sign(
        format!("AWS4{}", cfg.secret_key).as_bytes(),
        date_stamp.as_bytes(),
    );
    let region_key = hmac_sign(&date_key, cfg.region.as_bytes());
    let service_key = hmac_sign(&region_key, b"s3");
    let signing_key = hmac_sign(&service_key, b"aws4_request");

    // Sign the base64-encoded policy
    let signature = hex::encode(hmac_sign(&signing_key, policy_b64.as_bytes()));

    // Build the form fields
    let mut fields = BTreeMap::new();
    fields.insert("key".to_string(), object_key.to_string());
    fields.insert(
        "x-amz-algorithm".to_string(),
        "AWS4-HMAC-SHA256".to_string(),
    );
    fields.insert("x-amz-credential".to_string(), credential);
    fields.insert("x-amz-date".to_string(), amz_date);
    fields.insert("policy".to_string(), policy_b64);
    fields.insert("x-amz-signature".to_string(), signature);

    if !content_type.is_empty() {
        fields.insert("Content-Type".to_string(), content_type.to_string());
    }
    for (k, v) in metadata {
        fields.insert(k.clone(), v.clone());
    }

    // Build the URL
    let endpoint = cfg.endpoint.trim_end_matches('/');
    let url = format!("{}/{}", endpoint, cfg.bucket);

    Ok(PresignedPost { url, fields })
}

fn hmac_sign(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn format_amz_date(unix_secs: u64) -> String {
    // Format: YYYYMMDD'T'HHMMSS'Z'
    let secs_total = unix_secs;
    let days_since_epoch = secs_total / 86400;

    // Compute year/month/day from days since Unix epoch
    let (year, month, day) = civil_from_days(days_since_epoch as i64);
    let remaining = secs_total % 86400;
    let hours = remaining / 3600;
    let minutes = (remaining % 3600) / 60;
    let seconds = remaining % 60;

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

fn format_iso8601(unix_secs: u64) -> String {
    let secs_total = unix_secs;
    let days_since_epoch = secs_total / 86400;
    let (year, month, day) = civil_from_days(days_since_epoch as i64);
    let remaining = secs_total % 86400;
    let hours = remaining / 3600;
    let minutes = (remaining % 3600) / 60;
    let seconds = remaining % 60;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Convert days since Unix epoch (1970-01-01) to (year, month, day).
/// Uses the algorithm from Howard Hinnant.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_storage_config() -> StorageConfig {
        StorageConfig {
            endpoint: "http://127.0.0.1:9000".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            bucket: "shop-uploads".into(),
            region: "us-east-1".into(),
            public_base_url: None,
            presigned_expiry_secs: 3600,
        }
    }

    #[test]
    fn presigned_post_has_required_fields() {
        let cfg = test_storage_config();
        let post = build_presigned_post(
            &cfg,
            "uploads/test-file.png",
            "image/png",
            10 * 1024 * 1024,
            &BTreeMap::new(),
        )
        .unwrap();

        assert!(post.url.contains("127.0.0.1:9000"));
        assert!(post.url.contains("shop-uploads"));

        // Required SigV4 fields
        assert!(post.fields.contains_key("key"));
        assert_eq!(post.fields["key"], "uploads/test-file.png");
        assert!(post.fields.contains_key("x-amz-algorithm"));
        assert_eq!(post.fields["x-amz-algorithm"], "AWS4-HMAC-SHA256");
        assert!(post.fields.contains_key("x-amz-credential"));
        assert!(post.fields.contains_key("x-amz-date"));
        assert!(post.fields.contains_key("policy"));
        assert!(post.fields.contains_key("x-amz-signature"));

        // Signature should be 64 hex chars (SHA-256)
        assert_eq!(post.fields["x-amz-signature"].len(), 64);

        // Policy should be base64
        let policy_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &post.fields["policy"],
        )
        .unwrap();
        let policy: serde_json::Value = serde_json::from_slice(&policy_bytes).unwrap();
        assert!(policy["expiration"].is_string());
        assert!(policy["conditions"].is_array());
    }

    #[test]
    fn presigned_post_includes_content_type() {
        let cfg = test_storage_config();
        let post = build_presigned_post(
            &cfg,
            "data.json",
            "application/json",
            1024,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(post.fields["Content-Type"], "application/json");

        let policy_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &post.fields["policy"],
        )
        .unwrap();
        let policy: serde_json::Value = serde_json::from_slice(&policy_bytes).unwrap();
        let conditions = policy["conditions"].as_array().unwrap();
        let has_ct = conditions.iter().any(|c| {
            c.as_object()
                .and_then(|o| o.get("Content-Type"))
                .is_some_and(|v| v.as_str() == Some("application/json"))
        });
        assert!(has_ct);
    }

    #[test]
    fn presigned_post_includes_metadata() {
        let cfg = test_storage_config();
        let mut meta = BTreeMap::new();
        meta.insert("x-amz-meta-user".to_string(), "42".to_string());
        let post = build_presigned_post(&cfg, "obj", "", 100, &meta).unwrap();
        assert_eq!(post.fields["x-amz-meta-user"], "42");
    }

    #[test]
    fn presigned_post_policy_has_size_limit() {
        let cfg = test_storage_config();
        let post = build_presigned_post(&cfg, "obj", "", 5000, &BTreeMap::new()).unwrap();
        let policy_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &post.fields["policy"],
        )
        .unwrap();
        let policy: serde_json::Value = serde_json::from_slice(&policy_bytes).unwrap();
        let conditions = policy["conditions"].as_array().unwrap();
        let has_range = conditions.iter().any(|c| {
            c.as_array()
                .is_some_and(|arr| arr[0] == "content-length-range" && arr[2] == 5000)
        });
        assert!(has_range);
    }

    #[test]
    fn amz_date_format() {
        let s = format_amz_date(1719700000);
        assert_eq!(s.len(), 16);
        assert!(s.ends_with("Z"));
        assert_eq!(&s[8..9], "T");
    }

    #[test]
    fn iso8601_format() {
        let s = format_iso8601(1719700000);
        assert_eq!(s.len(), 20);
        assert!(s.ends_with("Z"));
        assert_eq!(&s[10..11], "T");
    }
}
