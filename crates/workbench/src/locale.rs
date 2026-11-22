pub(crate) fn select(mut get: impl FnMut(&str) -> Option<String>) -> String {
    ["LC_ALL", "LC_MESSAGES", "LANG"]
        .into_iter()
        .find_map(&mut get)
        .unwrap_or_default()
}

pub(crate) fn collator(
    locale: &str,
) -> Result<icu_collator::CollatorBorrowed<'static>, impl std::fmt::Display + std::fmt::Debug> {
    let locale = locale
        .split(['.', '@'])
        .next()
        .unwrap_or_default()
        .replace('_', "-");
    let locale = locale
        .parse::<icu_locale_core::Locale>()
        .map(icu_collator::CollatorPreferences::from)
        .unwrap_or_default();
    icu_collator::Collator::try_new(locale, Default::default())
}
