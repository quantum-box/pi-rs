use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Model prices in dollars per million tokens.
#[derive(Clone, Default, Deserialize, Serialize)]
struct ChatRates {
    input: Option<f64>,
    output: Option<f64>,
    #[serde(rename = "cacheRead")]
    cache_read: Option<f64>,
    #[serde(rename = "cacheWrite")]
    cache_write: Option<f64>,
}

impl ChatRates {
    fn overlay(self, newer: Self) -> Self {
        Self {
            input: newer.input.or(self.input),
            output: newer.output.or(self.output),
            cache_read: newer.cache_read.or(self.cache_read),
            cache_write: newer.cache_write.or(self.cache_write),
        }
    }

    fn valid(&self) -> bool {
        [self.input, self.output, self.cache_read, self.cache_write]
            .into_iter()
            .flatten()
            .all(|amount| amount.is_finite() && amount >= 0.0)
    }

    fn value(&self) -> Value {
        json!({
            "input": self.input.unwrap_or_default(),
            "output": self.output.unwrap_or_default(),
            "cacheRead": self.cache_read.unwrap_or_default(),
            "cacheWrite": self.cache_write.unwrap_or_default(),
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PriceTier {
    input_tokens_above: u64,
    #[serde(flatten)]
    rates: ChatRates,
}

/// Catalog pricing in USD per million tokens, including optional input tiers.
#[derive(Clone, Default, Deserialize, Serialize)]
pub struct ChatPrice {
    #[serde(flatten)]
    rates: ChatRates,
    tiers: Option<Vec<PriceTier>>,
}

impl ChatPrice {
    pub fn overlay(self, newer: Self) -> Self {
        Self {
            rates: self.rates.overlay(newer.rates),
            tiers: newer.tiers.or(self.tiers),
        }
    }

    pub fn valid(&self) -> bool {
        self.rates.valid()
            && self
                .tiers
                .as_deref()
                .unwrap_or_default()
                .iter()
                .all(|tier| tier.rates.valid())
    }

    pub fn value(&self) -> Value {
        let mut value = self.rates.value();
        if let Some(tiers) = &self.tiers {
            value["tiers"] = Value::Array(
                tiers
                    .iter()
                    .map(|tier| {
                        let mut value = tier.rates.value();
                        value["inputTokensAbove"] = json!(tier.input_tokens_above);
                        value
                    })
                    .collect(),
            );
        }
        value
    }

    pub fn estimate(
        &self,
        usage: crate::Usage,
        metadata: &crate::ModelMetadata,
    ) -> Option<crate::UsageCostBreakdown> {
        let tier = self
            .tiers
            .as_deref()
            .unwrap_or_default()
            .iter()
            .rev()
            .filter(|tier| usage.input_tokens > tier.input_tokens_above)
            .max_by_key(|tier| tier.input_tokens_above);
        let rates = tier.map_or(&self.rates, |tier| &tier.rates);
        let reads = metadata
            .cached_input_tokens
            .unwrap_or_default()
            .min(usage.input_tokens);
        let other_input = usage.input_tokens - reads;
        let writes = metadata
            .cache_creation_input_tokens
            .unwrap_or_default()
            .min(other_input);
        let long = metadata
            .cache_write_1h_tokens
            .unwrap_or_default()
            .min(writes);
        let input_rate = rates.input.unwrap_or_default();
        crate::UsageCostBreakdown::new(
            (input_rate / 1_000_000.0) * (other_input - writes) as f64,
            (rates.output.unwrap_or_default() / 1_000_000.0) * usage.output_tokens as f64,
            (rates.cache_read.unwrap_or_default() / 1_000_000.0) * reads as f64,
            (rates.cache_write.unwrap_or_default() / 1_000_000.0) * (writes - long) as f64
                + (input_rate / 500_000.0) * long as f64,
        )
    }
}
