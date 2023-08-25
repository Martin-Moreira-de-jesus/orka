use std::{sync::Arc, collections::HashMap};

use anyhow::Result;
use reqwest::Client;
use serde_json::Value;

const AUTH_TOKEN_KEY: &str = "access_token";

pub struct ManifestClient {}

impl ManifestClient {
    pub async fn get_auth_token(&self, repo: &str) -> Result<String> {
        let client = Client::new();

        let params = [
            ("service", "registry.docker.io"),
            ("scope", &format!("repository:{}:pull", repo)),
        ];

        let res = client
            .get("https://auth.docker.io/token")
            .query(&params)
            .send()
            .await?;

        let res_content = res
            .json::<HashMap<String, Value>>()
            .await?;

        Ok(res_content.get(AUTH_TOKEN_KEY).unwrap().to_string())
    }
}
