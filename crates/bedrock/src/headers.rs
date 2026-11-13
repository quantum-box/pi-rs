//! Pi-compatible Bedrock request-header filtering.
use std::collections::BTreeMap;

pub(crate) fn filtered(headers: http::HeaderMap) -> pi_rs_core::Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for (name, value) in headers.iter() {
        let name = name.as_str();
        if name == "authorization" || name == "host" || name.starts_with("x-amz-") {
            continue;
        }
        let value = value
            .to_str()
            .map_err(|_| pi_rs_core::Error::Config("invalid Bedrock custom header value".into()))?;
        result.insert(name.to_owned(), value.to_owned());
    }
    Ok(result)
}
