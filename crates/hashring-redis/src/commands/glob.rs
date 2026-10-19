//! Redis' glob patterns, for `KEYS` and `CONFIG GET`: `*`, `?`, character
//! classes such as `[a-z]` and `[^x]`, and a backslash escape. Classes parse
//! exactly as Redis' `stringmatchlen` parses them, edge cases included.
//!
//! A pattern is compiled once, so a character class is a bit lookup rather
//! than a reparse each time a `*` retries. Compiling costs a bounded amount
//! per pattern byte, ranges included, since it too runs where no deadline can
//! interrupt it. Matching is over bytes, since keys
//! are, and takes at most one step per pattern element per key byte. `KEYS`
//! applies one client-chosen pattern to every key in the cluster
//! synchronously, where no deadline can interrupt it, so it matches within a
//! step budget and gives up when the budget runs out.

use hashring_core::limits::DEFAULT_MAX_KEY_BYTES;

/// Every element but `*` consumes exactly one byte, so a pattern with more of
/// them than the longest key can hold matches nothing, and compiling it any
/// further would only spend the gateway's memory.
const MAX_FIXED_ELEMENTS: usize = DEFAULT_MAX_KEY_BYTES;

enum Element {
    /// `*`, any run of bytes. Consecutive stars are one.
    Star,
    /// `?`, exactly one byte.
    AnyByte,
    Byte(u8),
    /// The bytes a class accepts, one bit each.
    Class([u64; 4]),
}

pub struct Pattern {
    elements: Vec<Element>,
    /// Longer than any key could match.
    impossible: bool,
}

impl Pattern {
    pub fn new(pattern: &[u8]) -> Self {
        let mut elements = Vec::new();
        let mut fixed = 0;
        let mut p = 0;
        while p < pattern.len() {
            let element = match pattern[p] {
                b'*' => {
                    p += 1;
                    if matches!(elements.last(), Some(Element::Star)) {
                        continue;
                    }
                    elements.push(Element::Star);
                    continue;
                }
                b'?' => {
                    p += 1;
                    Element::AnyByte
                }
                b'[' => {
                    let (class, next) = class(pattern, p + 1);
                    p = next;
                    Element::Class(class)
                }
                b'\\' if p + 1 < pattern.len() => {
                    p += 2;
                    Element::Byte(pattern[p - 1])
                }
                literal => {
                    p += 1;
                    Element::Byte(literal)
                }
            };
            fixed += 1;
            if fixed > MAX_FIXED_ELEMENTS {
                return Self {
                    elements: Vec::new(),
                    impossible: true,
                };
            }
            elements.push(element);
        }
        Self {
            elements,
            impossible: false,
        }
    }

    /// Whether `text` matches all of the pattern.
    pub fn matches(&self, text: &[u8]) -> bool {
        let mut unlimited = u64::MAX;
        self.matches_within(text, &mut unlimited)
            .expect("an unlimited budget does not run out")
    }

    /// Whether `text` matches, spending one unit of `budget` per step; `None`
    /// if the budget ran out first.
    pub fn matches_within(&self, text: &[u8], budget: &mut u64) -> Option<bool> {
        if self.impossible {
            return Some(false);
        }
        let elements = &self.elements;
        let (mut p, mut t) = (0, 0);
        // The element just past the last `*`, and the text position that `*`
        // has consumed up to. On a mismatch the `*` takes one more byte.
        let mut star: Option<(usize, usize)> = None;
        while t < text.len() {
            *budget = budget.checked_sub(1)?;
            let matched = match elements.get(p) {
                Some(Element::Star) => {
                    p += 1;
                    star = Some((p, t));
                    continue;
                }
                Some(Element::AnyByte) => true,
                Some(Element::Byte(byte)) => *byte == text[t],
                Some(Element::Class(accepts)) => accepts_byte(accepts, text[t]),
                None => false,
            };
            if matched {
                p += 1;
                t += 1;
                continue;
            }
            let Some((after_star, consumed)) = star else {
                return Some(false);
            };
            p = after_star;
            t = consumed + 1;
            star = Some((after_star, t));
        }
        Some(
            elements[p..]
                .iter()
                .all(|element| matches!(element, Element::Star)),
        )
    }
}

fn accepts_byte(class: &[u64; 4], byte: u8) -> bool {
    class[usize::from(byte / 64)] & (1 << (byte % 64)) != 0
}

fn accept(class: &mut [u64; 4], byte: u8) {
    class[usize::from(byte / 64)] |= 1 << (byte % 64);
}

/// Accepts every byte from `low` to `high`, a word at a time, so a range costs
/// the same however many bytes it spans.
fn accept_range(class: &mut [u64; 4], low: u8, high: u8) {
    for (index, word) in class.iter_mut().enumerate() {
        let base = index * 64;
        let from = usize::from(low).max(base);
        let to = usize::from(high).min(base + 63);
        if from > to {
            continue;
        }
        let width = to - from + 1;
        let bits = if width == 64 {
            u64::MAX
        } else {
            (1_u64 << width) - 1
        };
        *word |= bits << (from - base);
    }
}

/// A range as Redis reads it. `stringmatchlen` compares C `char`s, which are
/// signed on x86-64, the platform Redis is overwhelmingly deployed on: so
/// `a-\xff` spans -1 to 97 and leaves `b` out. In unsigned terms a range that
/// crosses zero is two stretches, from 0 up and from 0x80 up to 0xff.
fn accept_redis_range(class: &mut [u64; 4], first: u8, second: u8) {
    let (first, second) = (first as i8, second as i8);
    let (low, high) = (first.min(second), first.max(second));
    if low >= 0 || high < 0 {
        accept_range(class, low as u8, high as u8);
    } else {
        accept_range(class, 0, high as u8);
        accept_range(class, low as u8, u8::MAX);
    }
}

/// Compiles the class whose body starts at `p`: the bytes it accepts, and
/// where the pattern continues after it. In the order Redis checks them: an
/// escape, the closing `]`, the end of the pattern (which closes the class
/// too), a range `x-y` (whose `y` may be `]`), and a literal.
fn class(pattern: &[u8], mut p: usize) -> ([u64; 4], usize) {
    let mut accepts = [0_u64; 4];
    // Only `^` negates, as in Redis: `[!a]` is a class of `!` and `a`.
    let negated = pattern.get(p) == Some(&b'^');
    if negated {
        p += 1;
    }
    let next = loop {
        match pattern.get(p) {
            Some(b'\\') if p + 1 < pattern.len() => {
                accept(&mut accepts, pattern[p + 1]);
                p += 2;
            }
            Some(b']') => break p + 1,
            None => break p,
            Some(&first) if pattern.get(p + 1) == Some(&b'-') && p + 2 < pattern.len() => {
                // Redis accepts a range written backwards.
                accept_redis_range(&mut accepts, first, pattern[p + 2]);
                p += 3;
            }
            Some(&literal) => {
                accept(&mut accepts, literal);
                p += 1;
            }
        }
    };
    if negated {
        for word in accepts.iter_mut() {
            *word = !*word;
        }
    }
    (accepts, next)
}

#[cfg(test)]
mod tests {
    use super::Pattern;

    fn m(pattern: &str, text: &str) -> bool {
        Pattern::new(pattern.as_bytes()).matches(text.as_bytes())
    }

    #[test]
    fn wildcards_match_the_way_redis_documents_them() {
        assert!(m("*", ""));
        assert!(m("*", "anything"));
        assert!(m("h?llo", "hello") && m("h?llo", "hallo") && !m("h?llo", "hllo"));
        assert!(m("h*llo", "hllo") && m("h*llo", "heeeello"));
        assert!(m("h[ae]llo", "hello") && m("h[ae]llo", "hallo") && !m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo") && !m("h[^e]llo", "hello"));
        assert!(m("h[a-b]llo", "hbllo") && !m("h[a-b]llo", "hcllo"));
        assert!(m("h[b-a]llo", "hallo"), "a backwards range still matches");
        assert!(m("user:*:name", "user:42:name") && !m("user:*:name", "user:42:email"));
        assert!(!m("abc", "ab") && !m("ab", "abc"));
    }

    #[test]
    fn only_a_caret_negates_a_class() {
        assert!(m("[!a]", "a") && m("[!a]", "!") && !m("[!a]", "b"));
    }

    #[test]
    fn a_backslash_escapes_a_special_character() {
        assert!(m(r"a\*b", "a*b") && !m(r"a\*b", "axb"));
        assert!(m(r"[\]]", "]"));
        assert!(m("a\\", "a\\"), "a trailing backslash is literal");
    }

    #[test]
    fn classes_parse_their_edge_cases_as_redis_does() {
        // A range may end in `]`; written backwards, `a-]` spans `]` to `a`.
        assert!(m("[a-]]", "_") && !m("[a-]]", "b"));
        // The end of the pattern closes a class and keeps what it gathered.
        assert!(m("[abc", "a") && !m("[abc", "x"));
        assert!(m("[^abc", "x") && !m("[^abc", "a"));
        // `[]` closes at once: an empty class, which matches nothing.
        assert!(!m("[]", "]") && !m("[]", ""));
    }

    #[test]
    fn a_binary_range_orders_its_ends_as_redis_on_x86_64_does() {
        let matches = |pattern: &[u8], text: &[u8]| Pattern::new(pattern).matches(text);
        // `a-\xff` is 97 to -1, so -1 to 97: every ASCII byte up to `a`, and
        // 0xff, but not `b` and not the rest of the high half.
        assert!(matches(b"[a-\xff]", b"a") && matches(b"[a-\xff]", b"\xff"));
        assert!(matches(b"[a-\xff]", b"\0"));
        assert!(!matches(b"[a-\xff]", b"b") && !matches(b"[a-\xfe]", b"\x80"));
        // Within the high half the order is the usual one.
        assert!(matches(b"[\x80-\xff]", b"\x90") && !matches(b"[\x80-\xff]", b"a"));
    }

    #[test]
    fn a_range_mask_covers_exactly_its_span() {
        for (low, high) in [(0, 255), (0, 63), (63, 64), (5, 5), (100, 200), (192, 255)] {
            let mut class = [0_u64; 4];
            super::accept_range(&mut class, low, high);
            for byte in 0..=255_u8 {
                assert_eq!(
                    super::accepts_byte(&class, byte),
                    (low..=high).contains(&byte),
                    "{low}-{high} at {byte}"
                );
            }
        }
    }

    #[test]
    fn many_stars_do_not_backtrack_exponentially() {
        let text = "a".repeat(10_000);
        assert!(!m("*a*a*a*a*a*a*a*a*a*a*b", &text));
        assert!(m("*a*a*a*a*a*a*a*a*a*a*", &text));
    }

    #[test]
    fn a_large_class_costs_one_step_per_byte() {
        // Compiled once, a class of a million bytes is one bit lookup.
        let pattern = format!("*[{}]", "a".repeat(1 << 20));
        let pattern = Pattern::new(pattern.as_bytes());
        let mut budget = 1_000_000;
        assert_eq!(
            pattern.matches_within(&[b'x'; 1024], &mut budget),
            Some(false)
        );
    }

    #[test]
    fn a_pattern_longer_than_any_key_is_not_compiled_out() {
        // Eight million empty classes would be gigabytes of tables; the
        // pattern stops compiling once it outgrows the longest key.
        let pattern = Pattern::new("[]".repeat(1 << 23).as_bytes());
        assert!(pattern.impossible && pattern.elements.is_empty());
        assert!(!pattern.matches(b"anything"));
        // Stars consume nothing, so any number of them still compiles.
        let stars = Pattern::new(&[b'*'; 1 << 20]);
        assert!(!stars.impossible && stars.matches(b"anything"));
    }

    #[test]
    fn matching_stops_when_the_budget_runs_out() {
        let pattern = Pattern::new(b"*a*a*a*b");
        let mut budget = 100;
        assert_eq!(pattern.matches_within(&[b'a'; 10_000], &mut budget), None);
    }
}
