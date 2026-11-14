//! Provider-backed model catalog presentation.
use pi_rs::Result;
use pi_rs::ollama::ModelDetails;
use serde_json::{Value, json};
use unicode_width::UnicodeWidthStr;

use super::{Cli, OutputMode, model_selection, output};

pub(super) async fn list(args: &Cli, search: &str) -> Result<()> {
    let mut catalogs = if args.connection_explicit {
        vec![
            model_selection::catalog(args, args.provider.clone(), args.allow_model_probes())
                .await?,
        ]
    } else {
        model_selection::catalogs(args, true)
            .await
            .unwrap_or_default()
    };
    catalogs.sort_by(|left, right| left.provider.id().cmp(right.provider.id()));
    let catalog_empty = catalogs.iter().all(|catalog| catalog.models.is_empty());
    let mut rows = Vec::new();
    for catalog in catalogs {
        let provider = catalog.provider.id();
        let mut models: Vec<_> = catalog
            .models
            .iter()
            .filter(|model| list_matches(search, provider, model))
            .cloned()
            .collect();
        models.sort();
        let mut details = catalog.details(&models, &args.model_profiles).await;
        for model in models {
            let detail = details.remove(&model);
            if args.mode == Some(OutputMode::Json) {
                let mut row = json!({
                    "type":"model", "model":model, "name":catalog.display_name(&model),
                    "provider":provider,
                });
                if let Some(metadata) = args.model_profiles.model(&catalog.provider, &model) {
                    metadata.apply_catalog_metadata(&mut row);
                }
                output(with_details(row, detail.as_ref()));
            } else {
                rows.push(table_row(provider, model, detail.as_ref()));
            }
        }
    }
    if args.mode != Some(OutputMode::Json) {
        if catalog_empty {
            println!("No models available.");
        } else if rows.is_empty() {
            println!("No models matching \"{search}\"");
        } else {
            println!("{}", table(rows));
        }
    }
    Ok(())
}

// Pi's fuzzy filter retries ASCII letter/digit tokens in the opposite order
// when their normal subsequence does not match. The searchable text consists
// of the provider and model ID, not the display name.
fn list_matches(search: &str, provider: &str, model: &str) -> bool {
    let candidate = format!("{provider} {model}");
    search
        .split(|c: char| ((c.is_whitespace() && c != '\u{85}') || c == '\u{feff}') || c == '/')
        .filter(|token| !token.is_empty())
        .all(|token| {
            if model_selection::fuzzy(token, &candidate) {
                return true;
            }
            let token = token.to_lowercase();
            let bytes = token.as_bytes();
            let split = bytes.iter().position(|c| !c.is_ascii_alphabetic());
            let split = split
                .filter(|&i| i > 0 && i < bytes.len() && bytes[i..].iter().all(u8::is_ascii_digit))
                .or_else(|| {
                    let i = bytes.iter().position(|c| !c.is_ascii_digit())?;
                    (i > 0 && i < bytes.len() && bytes[i..].iter().all(u8::is_ascii_alphabetic))
                        .then_some(i)
                });
            split.is_some_and(|i| {
                model_selection::fuzzy(&format!("{}{}", &token[i..], &token[..i]), &candidate)
            })
        })
}

pub(super) fn with_details(mut model: Value, details: Option<&ModelDetails>) -> Value {
    if let Some(details) = details
        && let Ok(Value::Object(fields)) = serde_json::to_value(details)
        && let Value::Object(model) = &mut model
    {
        model.extend(fields);
    }
    model
}

fn format_tokens(tokens: u32) -> String {
    let (scale, unit) = if tokens >= 1_000_000 {
        (1_000_000, "M")
    } else if tokens >= 1000 {
        (1000, "K")
    } else {
        return tokens.to_string();
    };
    if tokens % scale == 0 {
        format!("{}{unit}", tokens / scale)
    } else {
        format!("{:.1}{unit}", f64::from(tokens) / f64::from(scale))
    }
}

fn capability(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "yes",
        Some(false) => "no",
        None => "?",
    }
}

fn table_row(provider: &str, model: String, info: Option<&ModelDetails>) -> [String; 6] {
    [
        provider.into(),
        model,
        info.and_then(|info| info.context_window)
            .map_or_else(|| "?".into(), format_tokens),
        info.and_then(|info| info.max_output_tokens)
            .map_or_else(|| "?".into(), format_tokens),
        capability(info.and_then(|info| info.reasoning)).into(),
        capability(info.and_then(|info| info.images)).into(),
    ]
}

fn table(mut rows: Vec<[String; 6]>) -> String {
    rows.insert(
        0,
        [
            "provider", "model", "context", "max-out", "thinking", "images",
        ]
        .map(str::to_owned),
    );
    let mut widths = [0; 6];
    for row in &rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.width());
        }
    }
    rows.iter()
        .map(|row| {
            row.iter()
                .zip(widths)
                .map(|(cell, width)| format!("{cell}{}", " ".repeat(width - cell.width())))
                .collect::<Vec<_>>()
                .join("  ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzzy_list_search_matches_pi_token_swaps_and_searchable_fields() {
        assert!(list_matches("p2", "ollama", "nova-2p"));
        assert!(list_matches("2p", "ollama", "nova-p2"));
        assert!(list_matches("K2", "ollama", "nova-2k"));
        assert!(list_matches("oll / p2", "ollama", "nova-2p"));
        assert!(list_matches("OLL\u{feff}p2", "ollama", "nova-2p"));
        assert!(!list_matches("p2", "ollama", "nova-4x"));
        assert!(!list_matches("p-2", "ollama", "nova-2p"));
        assert!(!list_matches("SparkName", "ollama", "nova-2p"));
        assert!(!list_matches("oll\u{85}p2", "ollama", "nova-2p"));
    }

    #[test]
    fn table_distinguishes_missing_metadata_from_reported_capabilities() {
        let details = ModelDetails {
            context_window: Some(131_072),
            max_output_tokens: None,
            reasoning: Some(true),
            images: Some(false),
            thinking_levels: None,
        };
        let result = table(vec![
            table_row("ollama", "日本語".into(), Some(&details)),
            table_row("ollama", "fallback".into(), None),
        ]);
        let rows: Vec<_> = result.lines().collect();
        assert!(rows[0].contains("max-out"));
        assert!(rows[1].contains("131.1K"));
        assert!(rows[1].contains("yes") && rows[1].contains("no"));
        assert_eq!(rows[2].matches('?').count(), 4);
        assert!(rows.iter().all(|row| row.width() == rows[0].width()));
    }
}
