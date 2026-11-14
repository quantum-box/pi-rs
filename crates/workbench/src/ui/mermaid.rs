//! Bounded native rendering for the Mermaid families supported by Pi.
use std::collections::VecDeque;
use std::sync::{Arc, LazyLock, Mutex};

use unicode_width::UnicodeWidthStr;

use super::{
    mermaid_engine::{self, Cls, MermaidArt},
    render::{Line, Span, clean},
    theme::Theme,
};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum Mermaid {
    #[default]
    Off,
    Streaming,
    Final,
}

struct Diagram {
    art: MermaidArt,
    warnings: Vec<String>,
}

type Cache = VecDeque<(String, Option<Arc<Diagram>>)>;
static CACHE: LazyLock<Mutex<Cache>> = LazyLock::new(Mutex::default);

fn draw(source: &str) -> Option<MermaidArt> {
    let art = mermaid_engine::render(source, None)?;
    (!art.plain_lines.is_empty()
        && art.plain_lines.iter().map(String::len).sum::<usize>() <= 128 * 1024)
        .then_some(art)
}

fn diagram(source: &str) -> Option<Arc<Diagram>> {
    if source.len() > 16 * 1024 || source.lines().take(129).count() > 128 {
        return None;
    }
    let mut cache = CACHE.lock().expect("Mermaid render cache");
    if let Some(index) = cache.iter().position(|(key, _)| key == source) {
        let entry = cache.remove(index).expect("cached diagram");
        let value = entry.1.clone();
        cache.push_back(entry);
        return value;
    }
    let safe = clean(source);
    let value = draw(&safe)
        .map(|art| Diagram {
            art,
            warnings: mermaid_engine::flowchart_warnings(&safe),
        })
        .or_else(|| {
            let (prefix, tail) = safe.trim_end().rsplit_once('\n')?;
            Some(Diagram {
                art: draw(prefix)?,
                warnings: vec![format!(
                    "dropped, unreadable final line: \"{}\"",
                    tail.trim()
                )],
            })
        })
        .map(Arc::new);
    if cache.len() == 24 {
        cache.pop_front();
    }
    cache.push_back((source.into(), value.clone()));
    value
}

pub fn render(
    source: &str,
    theme: &Theme,
    width: usize,
    mode: Mermaid,
) -> (Option<Vec<Line>>, Option<String>) {
    if mode == Mermaid::Off {
        return (None, None);
    }
    let Some(diagram) = diagram(source).filter(|diagram| {
        diagram
            .art
            .plain_lines
            .iter()
            .all(|row| UnicodeWidthStr::width(row.as_str()) <= width)
    }) else {
        return (None, None);
    };
    if mode == Mermaid::Final
        && let Some(warning) = diagram.warnings.first()
    {
        return (
            None,
            Some(format!(
                "Mermaid diagram not rendered: {warning}{}",
                if diagram.warnings.len() > 1 {
                    format!(" (+{} more)", diagram.warnings.len() - 1)
                } else {
                    String::new()
                }
            )),
        );
    }
    let lines = diagram
        .art
        .styled_lines
        .iter()
        .map(|row| Line {
            spans: row
                .iter()
                .map(|(text, class)| {
                    let color = match class {
                        Cls::Border => "borderMuted",
                        Cls::Edge => "accent",
                        Cls::EdgeLabel => "muted",
                        Cls::Text | Cls::Empty => "text",
                    };
                    Span::themed(text, theme, color)
                })
                .collect(),
            ..Line::default()
        })
        .collect();
    (Some(lines), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_renderer_covers_the_reference_diagram_families() {
        let theme = Theme::system();
        for (source, label) in [
            ("flowchart LR\nA[Build] --> B[Ship]", "Build"),
            ("sequenceDiagram\nAlice->>Bob: Hello", "Alice"),
            ("stateDiagram-v2\n[*] --> Idle", "Idle"),
            ("classDiagram\nAnimal <|-- Dog", "Animal"),
            ("erDiagram\nUSER ||--o{ ORDER : places", "USER"),
        ] {
            let (lines, warning) = render(source, &theme, 120, Mermaid::Final);
            assert!(warning.is_none(), "{source}: {warning:?}");
            let lines = lines.unwrap_or_else(|| panic!("could not render {source}"));
            assert!(
                lines
                    .iter()
                    .flat_map(|line| &line.spans)
                    .any(|span| span.text.contains(label)),
                "{source}"
            );
        }
    }

    #[test]
    fn flowchart_partial_render_warns_only_after_streaming() {
        let theme = Theme::system();
        let source = "flowchart LR\nA[Foo] invalid\nB[Bar] also-invalid";
        let (lines, warning) = render(source, &theme, 120, Mermaid::Final);
        assert!(lines.is_none());
        assert_eq!(
            warning.as_deref(),
            Some("Mermaid diagram not rendered: dropped, expected a link: \"invalid\" (+1 more)")
        );
        let (lines, warning) = render(source, &theme, 120, Mermaid::Streaming);
        assert!(lines.is_some());
        assert!(warning.is_none());
        let (lines, warning) = render(
            "flowchart LR\nA[Foo]:::highlight-->B[Bar]",
            &theme,
            120,
            Mermaid::Final,
        );
        assert!(lines.is_some());
        assert!(warning.is_none());
        let source = "flowchart LR\nA[unfinished";
        let (lines, warning) = render(source, &theme, 120, Mermaid::Final);
        assert!(lines.is_none());
        assert!(warning.as_deref().is_some_and(|warning| warning
            == "Mermaid diagram not rendered: node \"A\": label is missing its closing `]`"));
        let (lines, warning) = render(source, &theme, 120, Mermaid::Streaming);
        assert!(lines.is_some());
        assert!(warning.is_none());
    }

    #[test]
    fn falls_back_to_source_for_disabled_unsupported_or_overwide_diagrams() {
        let theme = Theme::system();
        let source = "graph LR\nA[Build module] --> B[Ship code]";
        assert!(render(source, &theme, 120, Mermaid::Off).0.is_none());
        assert!(render(source, &theme, 10, Mermaid::Final).0.is_none());
        assert!(
            render("pie\n\"Test\": 3", &theme, 120, Mermaid::Final)
                .0
                .is_none()
        );
    }
}
