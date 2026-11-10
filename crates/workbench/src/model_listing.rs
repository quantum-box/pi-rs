//! Provider-backed model catalog presentation.
use std::collections::BTreeMap;

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
    let provider = args.provider.id();
    let catalog =
        model_selection::catalog(args, args.provider.clone(), args.allow_model_probes()).await?;
    let catalog_empty = catalog.models.is_empty();
    let models: Vec<_> = catalog
        .models
        .iter()
        .filter(|model| {
            let configured_name = args
                .model_profiles
                .model(&args.provider, model)
                .and_then(|metadata| metadata.name)
                .unwrap_or_default();
            let candidate = format!("{provider} {model} {configured_name}");
            terms
                .iter()
                .all(|term| model_selection::fuzzy(term, &candidate))
        })
        .cloned()
        .collect();
    let details = catalog.details(&models, &args.model_profiles).await;
    if args.mode == Some(OutputMode::Json) {
        for model in models {
            let name = args
                .model_profiles
                .model(&args.provider, &model)
                .and_then(|metadata| metadata.name)
                .unwrap_or_else(|| model.clone());
            output(with_details(
                json!({"type":"model","model":model,"name":name,"provider":provider}),
                details.get(&model),
            ));
        }
    } else if catalog_empty {
        println!("No models available.");
    } else if models.is_empty() {
        println!("No models matching \"{search}\"");
    } else {
        println!("{}", table(provider, &models, &details));
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

fn table(provider: &str, models: &[String], details: &BTreeMap<String, ModelDetails>) -> String {
    let mut rows = vec![
        [
            "provider", "model", "context", "max-out", "thinking", "images",
        ]
        .map(str::to_owned),
    ];
    for model in models {
        let info = details.get(model);
        rows.push([
            provider.into(),
            model.clone(),
            info.and_then(|info| info.context_window)
                .map_or_else(|| "?".into(), format_tokens),
            info.and_then(|info| info.max_output_tokens)
                .map_or_else(|| "?".into(), format_tokens),
            capability(info.and_then(|info| info.reasoning)).into(),
            capability(info.and_then(|info| info.images)).into(),
        ]);
    }
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
        let models = vec!["日本語".into(), "fallback".into()];
        let details = BTreeMap::from([(
            "日本語".into(),
            ModelDetails {
                context_window: Some(131_072),
                max_output_tokens: None,
                reasoning: Some(true),
                images: Some(false),
                thinking_levels: None,
            },
        )]);
        let result = table("ollama", &models, &details);
        let rows: Vec<_> = result.lines().collect();
        assert!(rows[0].contains("max-out"));
        assert!(rows[1].contains("131.1K"));
        assert!(rows[1].contains("yes") && rows[1].contains("no"));
        assert_eq!(rows[2].matches('?').count(), 4);
        assert!(rows.iter().all(|row| row.width() == rows[0].width()));
    }
}
