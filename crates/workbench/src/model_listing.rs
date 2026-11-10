//! Provider-backed model catalog presentation.
use pi_rs::Result;
use pi_rs::ollama::ModelDetails;
use serde_json::{Value, json};
use unicode_width::UnicodeWidthStr;

use super::{Cli, OutputMode, model_selection, output};

pub(super) async fn list(args: &Cli, search: &str) -> Result<()> {
    let terms: Vec<_> = search
        .split(|character: char| character.is_whitespace() || character == '/')
        .filter(|term| !term.is_empty())
        .collect();
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
            .filter(|model| {
                let candidate = format!("{provider} {model} {}", catalog.display_name(model));
                terms
                    .iter()
                    .all(|term| model_selection::fuzzy(term, &candidate))
            })
            .cloned()
            .collect();
        models.sort();
        let mut details = catalog.details(&models, &args.model_profiles).await;
        for model in models {
            let detail = details.remove(&model);
            if args.mode == Some(OutputMode::Json) {
                output(with_details(
                    json!({
                        "type":"model", "model":model, "name":catalog.display_name(&model),
                        "provider":provider,
                    }),
                    detail.as_ref(),
                ));
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
