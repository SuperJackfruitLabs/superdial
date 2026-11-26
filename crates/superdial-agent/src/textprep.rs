//! Reply text made fit to be spoken: markdown removed, length capped. The room keeps the full
//! text; this is only what is said aloud.

use std::sync::LazyLock;

use regex::Regex;

/// About how much of a reply is spoken, in characters.
pub const SPOKEN_CAP: usize = 1200;

/// The reply as plain sentences, at most `cap` characters, cut at a sentence boundary.
pub fn speakable(text: &str, cap: usize) -> String {
    static RULE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^([-*_]\s*){3,}$").unwrap());
    static HEADING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^#{1,6}\s+(.*?)\s*#*$").unwrap());
    static QUOTE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(>\s?)+").unwrap());
    static LIST: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^([-*+]|\d{1,3}[.)])\s+").unwrap());

    let mut lines = Vec::new();
    let mut in_fence = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with("```") || line.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence || RULE.is_match(line) {
            continue;
        }
        let mut pause = false;
        let mut line = QUOTE.replace(line, "").into_owned();
        if let Some(c) = HEADING.captures(&line) {
            line = c[1].to_owned();
            pause = true;
        }
        if LIST.is_match(&line) {
            line = LIST.replace(&line, "").into_owned();
            pause = true;
        }
        let mut line = inline(&line).trim().to_owned();
        if line.is_empty() {
            continue;
        }
        if pause && !line.ends_with(['.', '!', '?', ':', ';', '…']) {
            line.push('.');
        }
        lines.push(line);
    }
    cap_at_sentence(&lines.join("\n"), cap)
}

/// Inline markdown to its text: images and links to their text, code to its content, emphasis
/// markers removed. A `*` or `_` that is not wrapped around a word is left alone.
fn inline(s: &str) -> String {
    static IMAGE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"!\[([^\]]*)\]\([^)]*\)").unwrap());
    static LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[([^\]]+)\]\([^)]*\)").unwrap());
    static AUTOLINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<((?:https?|mailto):[^>\s]+)>").unwrap());
    static CODE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"`+([^`]*)`+").unwrap());
    static STRONG: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\*\*(\S(?:.*?\S)??)\*\*").unwrap());
    static STRONG_UNDER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"__(\S(?:.*?\S)??)__").unwrap());
    static STRIKE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"~~(\S(?:.*?\S)??)~~").unwrap());
    static STAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\*(\S(?:[^*]*?\S)??)\*").unwrap());
    static UNDER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(^|[^\w])_(\S(?:[^_]*?\S)??)_($|[^\w])").unwrap());

    let s = IMAGE.replace_all(s, "$1");
    let s = LINK.replace_all(&s, "$1");
    let s = AUTOLINK.replace_all(&s, "$1");
    let s = CODE.replace_all(&s, "$1");
    let s = STRONG.replace_all(&s, "$1");
    let s = STRONG_UNDER.replace_all(&s, "$1");
    let s = STRIKE.replace_all(&s, "$1");
    let s = STAR.replace_all(&s, "$1");
    // Matches share their boundary character, so repeat until nothing changes.
    let mut s = s.into_owned();
    loop {
        let next = UNDER.replace_all(&s, "$1$2$3").into_owned();
        if next == s {
            return s;
        }
        s = next;
    }
}

/// At most `cap` characters: up to the last sentence end that fits, or, when no sentence ends
/// in the last three quarters of the allowance, up to the last whole word and an ellipsis.
fn cap_at_sentence(s: &str, cap: usize) -> String {
    if s.chars().count() <= cap {
        return s.to_owned();
    }
    let chars: Vec<char> = s.chars().collect();
    let mut end = None;
    for i in (0..cap).rev() {
        let next_is_break = chars.get(i + 1).is_none_or(|c| c.is_whitespace());
        if matches!(chars[i], '.' | '!' | '?') && next_is_break {
            end = Some(i + 1);
            break;
        }
    }
    if let Some(end) = end.filter(|e| *e >= cap / 4) {
        return chars[..end].iter().collect::<String>().trim_end().to_owned();
    }
    let room = cap.saturating_sub(1);
    let cut = (1..=room).rev().find(|&i| chars[i].is_whitespace()).unwrap_or(room);
    let head: String = chars[..cut].iter().collect();
    format!("{}…", head.trim_end().trim_end_matches([',', ';', ':', '-']))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_unchanged() {
        assert_eq!(
            speakable("Paris is the capital of France.", SPOKEN_CAP),
            "Paris is the capital of France."
        );
    }

    #[test]
    fn strips_headings_and_adds_pause() {
        assert_eq!(speakable("# Title\nBody text.", SPOKEN_CAP), "Title.\nBody text.");
        assert_eq!(speakable("### Done!\n", SPOKEN_CAP), "Done!");
    }

    #[test]
    fn strips_emphasis() {
        assert_eq!(speakable("**a** *b* __c__ _d_ ~~e~~", SPOKEN_CAP), "a b c d e");
        assert_eq!(
            speakable("call snake_case_word now", SPOKEN_CAP),
            "call snake_case_word now"
        );
        assert_eq!(speakable("2 * 3 * 4", SPOKEN_CAP), "2 * 3 * 4");
    }

    #[test]
    fn strips_list_markers() {
        assert_eq!(speakable("- one\n2. two\n* three!", SPOKEN_CAP), "one.\ntwo.\nthree!");
        assert_eq!(speakable("> quoted line", SPOKEN_CAP), "quoted line");
    }

    #[test]
    fn drops_code_fences_keeps_inline_code_text() {
        let md = "Run `cargo test` first.\n```sh\nrm -rf /\n```\nThen relax.";
        assert_eq!(speakable(md, SPOKEN_CAP), "Run cargo test first.\nThen relax.");
    }

    #[test]
    fn links_become_their_text() {
        assert_eq!(
            speakable(
                "See [the docs](https://x.example/a_b) and ![a chart](c.png).",
                SPOKEN_CAP
            ),
            "See the docs and a chart."
        );
        assert_eq!(
            speakable("Go to <https://x.example>.", SPOKEN_CAP),
            "Go to https://x.example."
        );
    }

    #[test]
    fn rules_and_blank_lines_go() {
        assert_eq!(speakable("One.\n\n---\n\nTwo.", SPOKEN_CAP), "One.\nTwo.");
    }

    #[test]
    fn caps_at_sentence_boundary() {
        let sentence = "This sentence is exactly fifty characters long ok. ";
        assert_eq!(sentence.len(), 51);
        let text = sentence.repeat(40);
        let out = speakable(&text, SPOKEN_CAP);
        assert!(out.chars().count() <= SPOKEN_CAP, "{}", out.len());
        assert!(out.chars().count() > SPOKEN_CAP - 60);
        assert!(out.ends_with("ok."), "{out}");
    }

    #[test]
    fn caps_without_sentence_cuts_at_word_and_marks_it() {
        let text = "word ".repeat(400);
        let out = speakable(&text, SPOKEN_CAP);
        assert!(out.chars().count() <= SPOKEN_CAP);
        assert!(out.ends_with("word…"), "{out}");
    }

    #[test]
    fn cap_counts_characters_not_bytes() {
        let text = "é".repeat(2000);
        let out = speakable(&text, 100);
        assert!(out.chars().count() <= 100);
    }
}
