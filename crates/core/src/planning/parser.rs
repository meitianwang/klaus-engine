//! Incremental parsing for standalone proposed-plan delimiter lines.

use crate::runtime_values::RuntimeString;

const OPEN: &str = "<proposed_plan>";
const CLOSE: &str = "</proposed_plan>";

/// Ordered assistant text and proposed-plan lifecycle events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanSegment {
    Text(RuntimeString),
    Start,
    Delta(RuntimeString),
    End,
}

// Coalesce each push in mutable storage. Freezing a RuntimeString for every
// code unit would repeatedly copy the growing segment.
enum BufferedSegment {
    Text(Vec<u16>),
    Start,
    Delta(Vec<u16>),
    End,
}

impl BufferedSegment {
    fn freeze(self) -> PlanSegment {
        match self {
            Self::Text(units) => PlanSegment::Text(RuntimeString::from_utf16(units)),
            Self::Start => PlanSegment::Start,
            Self::Delta(units) => PlanSegment::Delta(RuntimeString::from_utf16(units)),
            Self::End => PlanSegment::End,
        }
    }
}

#[derive(Debug, Default)]
enum Content {
    #[default]
    Text,
    Plan,
    Rejected,
}

/// Parses one assistant turn without retaining its ordinary text or plan body.
///
/// Delimiters must occupy an exact line, terminated by LF, CRLF, or the end of
/// the stream. Opening delimiters consume their line terminator. The single
/// line break immediately before a closing delimiter is structural and is
/// excluded from the plan; all other body code units are preserved verbatim.
///
/// A malformed turn must never be accepted, even if an earlier block emitted
/// `End`. Repeated blocks emit their bodies as ordinary text and no additional
/// lifecycle events. Exact malformed delimiter lines are hidden. A nested
/// opening invalidates the active block, which will never emit `End`.
#[derive(Debug)]
pub struct ProposedPlanParser {
    content: Content,
    candidate: Vec<u16>,
    checking_line: bool,
    pending_cr: bool,
    pending_break: &'static [u16],
    started: bool,
    completed: bool,
    malformed: bool,
}

impl Default for ProposedPlanParser {
    fn default() -> Self {
        Self {
            content: Content::Text,
            candidate: Vec::new(),
            checking_line: true,
            pending_cr: false,
            pending_break: &[],
            started: false,
            completed: false,
            malformed: false,
        }
    }
}

impl ProposedPlanParser {
    /// Scalar callers retain the existing entry point.
    pub fn push(&mut self, text: &str) -> Vec<PlanSegment> {
        self.push_units(text.encode_utf16())
    }

    /// Preserve exact normalized text, including a pair split between pushes.
    pub(crate) fn push_runtime(&mut self, text: &RuntimeString) -> Vec<PlanSegment> {
        self.push_units(text.as_utf16().iter().copied())
    }

    /// Withhold only a possible delimiter prefix, a CR and one plan-body line
    /// break. All other units stream through a mutable per-push segment buffer.
    fn push_units(&mut self, units: impl IntoIterator<Item = u16>) -> Vec<PlanSegment> {
        let mut output = Vec::new();
        for unit in units {
            if self.pending_cr {
                self.pending_cr = false;
                if unit == 0x000a {
                    self.line_end(&[0x000d, 0x000a], &mut output);
                    continue;
                }
                self.code_unit(0x000d, &mut output);
            }
            match unit {
                0x000d => self.pending_cr = true,
                0x000a => self.line_end(&[0x000a], &mut output),
                unit => self.code_unit(unit, &mut output),
            }
        }
        output.into_iter().map(BufferedSegment::freeze).collect()
    }

    /// Resolves the last line at the terminal stream boundary.
    ///
    /// An unterminated plan emits its remaining body without `End`. Callers
    /// must additionally reject interrupted turns, even if they received a
    /// complete block before the interruption. Repeated calls return no data.
    pub fn finish(&mut self) -> Vec<PlanSegment> {
        let mut output = Vec::new();
        if self.pending_cr {
            self.pending_cr = false;
            self.code_unit(0x000d, &mut output);
        }
        if self.checking_line && (self.delimiter(OPEN) || self.delimiter(CLOSE)) {
            self.line_end(&[], &mut output);
        } else {
            self.flush_break(&mut output);
            self.flush_candidate(&mut output);
        }
        output.into_iter().map(BufferedSegment::freeze).collect()
    }

    /// Starts a new provider text item while retaining the turn-wide proposal audit.
    pub fn finish_message(&mut self) -> Vec<PlanSegment> {
        let output = self.finish();
        if !matches!(self.content, Content::Text) {
            self.malformed = true;
        }
        self.checking_line = true;
        output
    }

    /// Whether a proposal is still open or started without a valid `End`.
    pub fn incomplete(&self) -> bool {
        !matches!(self.content, Content::Text) || (self.started && !self.completed)
    }

    /// Whether any exact delimiter appeared in an invalid position.
    pub fn malformed(&self) -> bool {
        self.malformed
    }

    fn delimiter(&self, delimiter: &str) -> bool {
        self.candidate.iter().copied().eq(delimiter.encode_utf16())
    }

    fn delimiter_prefix(&self, delimiter: &str) -> bool {
        self.candidate
            .iter()
            .copied()
            .eq(delimiter.encode_utf16().take(self.candidate.len()))
    }

    fn code_unit(&mut self, unit: u16, output: &mut Vec<BufferedSegment>) {
        if self.checking_line {
            self.candidate.push(unit);
            if self.delimiter_prefix(OPEN) || self.delimiter_prefix(CLOSE) {
                return;
            }
            self.flush_break(output);
            self.flush_candidate(output);
            self.checking_line = false;
        } else {
            self.emit(&[unit], output);
        }
    }

    fn line_end(&mut self, line_break: &'static [u16], output: &mut Vec<BufferedSegment>) {
        if self.checking_line && self.delimiter(OPEN) {
            self.flush_break(output);
            match self.content {
                Content::Text if !self.started && !self.malformed => {
                    self.started = true;
                    self.content = Content::Plan;
                    output.push(BufferedSegment::Start);
                }
                Content::Text => {
                    self.malformed = true;
                    self.content = Content::Rejected;
                }
                Content::Plan | Content::Rejected => self.malformed = true,
            }
            self.candidate.clear();
        } else if self.checking_line && self.delimiter(CLOSE) {
            self.pending_break = &[];
            match self.content {
                Content::Plan if !self.malformed => {
                    self.completed = true;
                    output.push(BufferedSegment::End);
                }
                Content::Text => self.malformed = true,
                Content::Plan | Content::Rejected => {}
            }
            self.content = Content::Text;
            self.candidate.clear();
        } else {
            self.flush_break(output);
            self.flush_candidate(output);
            match self.content {
                Content::Text => self.emit(line_break, output),
                Content::Plan | Content::Rejected => self.pending_break = line_break,
            }
        }
        self.checking_line = true;
    }

    fn flush_break(&mut self, output: &mut Vec<BufferedSegment>) {
        let line_break = std::mem::take(&mut self.pending_break);
        self.emit(line_break, output);
    }

    fn flush_candidate(&mut self, output: &mut Vec<BufferedSegment>) {
        let candidate = std::mem::take(&mut self.candidate);
        self.emit(&candidate, output);
    }

    fn emit(&self, units: &[u16], output: &mut Vec<BufferedSegment>) {
        if units.is_empty() {
            return;
        }
        match (&self.content, output.last_mut()) {
            (Content::Plan, Some(BufferedSegment::Delta(previous)))
            | (Content::Text | Content::Rejected, Some(BufferedSegment::Text(previous))) => {
                previous.extend_from_slice(units);
            }
            (Content::Plan, _) => output.push(BufferedSegment::Delta(units.to_vec())),
            (Content::Text | Content::Rejected, _) => {
                output.push(BufferedSegment::Text(units.to_vec()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PlanSegment, ProposedPlanParser, CLOSE, OPEN};

    fn coalesce(segments: Vec<PlanSegment>) -> Vec<PlanSegment> {
        let mut output = Vec::new();
        for segment in segments {
            match (output.last_mut(), segment) {
                (Some(PlanSegment::Text(previous)), PlanSegment::Text(text))
                | (Some(PlanSegment::Delta(previous)), PlanSegment::Delta(text)) => {
                    *previous = previous.concat(&text);
                }
                (_, segment) => output.push(segment),
            }
        }
        output
    }

    fn parse(chunks: &[&str]) -> (Vec<PlanSegment>, ProposedPlanParser) {
        let mut parser = ProposedPlanParser::default();
        let mut segments = Vec::new();
        for chunk in chunks {
            segments.extend(parser.push(chunk));
        }
        segments.extend(parser.finish());
        (coalesce(segments), parser)
    }

    fn assert_every_boundary(input: &str, expected: Vec<PlanSegment>) {
        let boundaries = input
            .char_indices()
            .map(|(index, _)| index)
            .chain(std::iter::once(input.len()))
            .collect::<Vec<_>>();
        for boundary in &boundaries {
            let (actual, parser) = parse(&[&input[..*boundary], &input[*boundary..]]);
            assert_eq!(actual, expected, "split at {boundary}");
            assert!(!parser.incomplete(), "split at {boundary}");
            assert!(!parser.malformed(), "split at {boundary}");
        }
        let chunks = boundaries
            .windows(2)
            .map(|pair| &input[pair[0]..pair[1]])
            .collect::<Vec<_>>();
        let (actual, parser) = parse(&chunks);
        assert_eq!(actual, expected, "one character per chunk");
        assert!(!parser.incomplete());
        assert!(!parser.malformed());
    }

    #[test]
    fn delimiters_and_order_survive_every_character_boundary() {
        assert_every_boundary(
            "Before.\n<proposed_plan>\n# Plan\n\n1. Inspect 🦀\n2. Implement\n</proposed_plan>\nAfter.",
            vec![
                PlanSegment::Text("Before.\n".into()),
                PlanSegment::Start,
                PlanSegment::Delta("# Plan\n\n1. Inspect 🦀\n2. Implement".into()),
                PlanSegment::End,
                PlanSegment::Text("After.".into()),
            ],
        );
    }

    #[test]
    fn crlf_and_unterminated_closing_line_survive_every_boundary() {
        assert_every_boundary(
            "Before\r\n<proposed_plan>\r\n# Plan\r\n\r\n正文\r\n</proposed_plan>",
            vec![
                PlanSegment::Text("Before\r\n".into()),
                PlanSegment::Start,
                PlanSegment::Delta("# Plan\r\n\r\n正文".into()),
                PlanSegment::End,
            ],
        );
    }

    #[test]
    fn inline_indented_and_suffixed_tags_remain_literal_text() {
        let input = "Inline <proposed_plan> body </proposed_plan>.\n <proposed_plan>\n<proposed_plan> suffix\n</proposed_plan> \n<proposed_planx>\n";
        assert_every_boundary(input, vec![PlanSegment::Text(input.into())]);
    }

    #[test]
    fn inline_tags_and_carriage_returns_in_plan_content_are_preserved() {
        assert_every_boundary(
            "<proposed_plan>\n# Plan\nUse <proposed_plan> literally.\nA\rB\r\n</proposed_plan>\n",
            vec![
                PlanSegment::Start,
                PlanSegment::Delta("# Plan\nUse <proposed_plan> literally.\nA\rB".into()),
                PlanSegment::End,
            ],
        );
    }

    #[test]
    fn possible_delimiters_do_not_flash_before_the_line_is_decided() {
        let mut parser = ProposedPlanParser::default();
        for character in OPEN.chars() {
            assert!(parser.push(&character.to_string()).is_empty());
        }
        assert_eq!(parser.push("\n"), vec![PlanSegment::Start]);
        assert_eq!(
            parser.push("body\n"),
            vec![PlanSegment::Delta("body".into())]
        );
        for character in CLOSE.chars() {
            assert!(parser.push(&character.to_string()).is_empty());
        }
        assert_eq!(parser.push("\n"), vec![PlanSegment::End]);
        assert!(!parser.incomplete());
    }

    #[test]
    fn rejected_prefix_is_released_as_soon_as_it_cannot_be_a_delimiter() {
        let mut parser = ProposedPlanParser::default();
        assert!(parser.push("<proposed_plan>").is_empty());
        assert_eq!(
            parser.push("x"),
            vec![PlanSegment::Text("<proposed_plan>x".into())]
        );
        assert!(parser.finish().is_empty());
        assert!(!parser.incomplete());
        assert!(!parser.malformed());
    }

    #[test]
    fn interruption_at_every_character_never_completes_an_unclosed_plan() {
        let input = "<proposed_plan>\n# Plan\nDo work\r\n</proposed_plan>";
        let opening_end = OPEN.len() + 1;
        for boundary in 0..input.len() {
            let (segments, parser) = parse(&[&input[..boundary]]);
            assert!(!segments.contains(&PlanSegment::End), "cut at {boundary}");
            assert_eq!(
                parser.incomplete(),
                boundary >= OPEN.len(),
                "cut at {boundary}"
            );
            assert!(!parser.malformed(), "cut at {boundary}");
            if boundary > opening_end {
                let body = segments
                    .iter()
                    .filter_map(|segment| match segment {
                        PlanSegment::Delta(text) => {
                            Some(text.try_to_utf8().expect("scalar fixture body"))
                        }
                        _ => None,
                    })
                    .collect::<String>();
                assert_eq!(body, input[opening_end..boundary], "cut at {boundary}");
            }
        }
    }

    #[test]
    fn second_block_is_malformed_and_has_no_second_lifecycle() {
        let input = "<proposed_plan>\none\n</proposed_plan>\nbetween\n<proposed_plan>\ntwo\n</proposed_plan>\nafter";
        for boundary in 0..=input.len() {
            let (segments, parser) = parse(&[&input[..boundary], &input[boundary..]]);
            assert_eq!(
                segments,
                vec![
                    PlanSegment::Start,
                    PlanSegment::Delta("one".into()),
                    PlanSegment::End,
                    PlanSegment::Text("between\ntwoafter".into()),
                ],
                "split at {boundary}"
            );
            assert!(parser.malformed(), "split at {boundary}");
            assert!(!parser.incomplete(), "split at {boundary}");
        }
    }

    #[test]
    fn nested_block_and_unmatched_close_cannot_be_accepted() {
        let (segments, parser) = parse(&[
            "<proposed_plan>\nouter\n<proposed_plan>\ninner\n</proposed_plan>\n</proposed_plan>",
        ]);
        assert_eq!(
            segments,
            vec![
                PlanSegment::Start,
                PlanSegment::Delta("outer\ninner".into()),
            ]
        );
        assert!(parser.incomplete());
        assert!(parser.malformed());

        let (segments, parser) =
            parse(&["</proposed_plan>\n<proposed_plan>\nbody\n</proposed_plan>"]);
        assert_eq!(segments, vec![PlanSegment::Text("body".into())]);
        assert!(!parser.incomplete());
        assert!(parser.malformed());
    }

    #[test]
    fn empty_plan_and_blank_lines_have_explicit_separator_behavior() {
        assert_every_boundary(
            "<proposed_plan>\n</proposed_plan>",
            vec![PlanSegment::Start, PlanSegment::End],
        );
        assert_every_boundary(
            "<proposed_plan>\n\nfirst\n\n\n</proposed_plan>\n",
            vec![
                PlanSegment::Start,
                PlanSegment::Delta("\nfirst\n\n".into()),
                PlanSegment::End,
            ],
        );
    }

    #[test]
    fn long_lines_stream_without_accumulating_the_body() {
        let mut parser = ProposedPlanParser::default();
        let text = "x".repeat(128 * 1024);
        assert_eq!(
            parser.push(&text),
            vec![PlanSegment::Text(text.clone().into())]
        );
        assert!(parser.candidate.is_empty());
        assert_eq!(
            parser.push("\n<proposed_plan>\n"),
            vec![PlanSegment::Text("\n".into()), PlanSegment::Start,]
        );
        assert_eq!(parser.push(&text), vec![PlanSegment::Delta(text.into())]);
        assert!(parser.candidate.is_empty());
        assert_eq!(parser.push("\n</proposed_plan>\n"), vec![PlanSegment::End]);
        assert!(parser.finish().is_empty());
        assert!(parser.finish().is_empty());
    }

    #[test]
    fn trailing_carriage_return_is_literal_and_not_a_line_terminator() {
        let (segments, parser) = parse(&["<proposed_plan>\r"]);
        assert_eq!(
            segments,
            vec![PlanSegment::Text("<proposed_plan>\r".into())]
        );
        assert!(!parser.incomplete());
        assert!(!parser.malformed());
        let (segments, parser) = parse(&["<proposed_plan>\nbody\r"]);
        assert_eq!(
            segments,
            vec![PlanSegment::Start, PlanSegment::Delta("body\r".into()),]
        );
        assert!(parser.incomplete());
        assert!(!parser.malformed());
    }
}

#[cfg(test)]
mod message_boundary_tests {
    use super::*;
    #[test]
    fn a_new_provider_message_starts_on_its_own_first_line() {
        let mut parser = ProposedPlanParser::default();
        assert_eq!(
            parser.push("Let me inspect"),
            vec![PlanSegment::Text("Let me inspect".into())]
        );
        assert!(parser.finish_message().is_empty());
        assert_eq!(
            parser.push("<proposed_plan>\n# Plan\n</proposed_plan>\n"),
            vec![
                PlanSegment::Start,
                PlanSegment::Delta("# Plan".into()),
                PlanSegment::End
            ]
        );
        assert!(!parser.malformed());
    }
}

#[cfg(test)]
mod runtime_unit_tests {
    use super::*;

    fn coalesce(segments: Vec<PlanSegment>) -> Vec<PlanSegment> {
        let mut output = Vec::new();
        for segment in segments {
            match segment {
                PlanSegment::Text(text) => match output.last_mut() {
                    Some(BufferedSegment::Text(previous)) => {
                        previous.extend_from_slice(text.as_utf16())
                    }
                    _ => output.push(BufferedSegment::Text(text.as_utf16().to_vec())),
                },
                PlanSegment::Delta(text) => match output.last_mut() {
                    Some(BufferedSegment::Delta(previous)) => {
                        previous.extend_from_slice(text.as_utf16())
                    }
                    _ => output.push(BufferedSegment::Delta(text.as_utf16().to_vec())),
                },
                PlanSegment::Start => output.push(BufferedSegment::Start),
                PlanSegment::End => output.push(BufferedSegment::End),
            }
        }
        output.into_iter().map(BufferedSegment::freeze).collect()
    }

    fn parse(chunks: &[&[u16]]) -> (Vec<PlanSegment>, ProposedPlanParser) {
        let mut parser = ProposedPlanParser::default();
        let mut output = Vec::new();
        for chunk in chunks {
            output.extend(parser.push_runtime(&RuntimeString::from_utf16(chunk.to_vec())));
            assert!(
                parser.candidate.len() <= CLOSE.len(),
                "only an ASCII delimiter prefix remains buffered"
            );
        }
        output.extend(parser.finish());
        (coalesce(output), parser)
    }

    #[test]
    fn exact_utf16_payload_survives_every_code_unit_split_and_crlf_boundary() {
        let before = RuntimeString::from_utf16(vec![0xdc00, 0x000d, 0x000a]);
        let body =
            RuntimeString::from_utf16(vec![0xd800, 0x0078, 0xdc00, 0xd83e, 0xdd80, 0x000d, 0x0079]);
        let after = RuntimeString::from_utf16(vec![0xd800, 0x000d]);
        let input = before
            .concat(&"<proposed_plan>\r\n".into())
            .concat(&body)
            .concat(&"\r\n</proposed_plan>\r\n".into())
            .concat(&after);
        let expected = vec![
            PlanSegment::Text(before),
            PlanSegment::Start,
            PlanSegment::Delta(body),
            PlanSegment::End,
            PlanSegment::Text(after),
        ];
        for split in 0..=input.len_utf16() {
            let (actual, parser) = parse(&[&input.as_utf16()[..split], &input.as_utf16()[split..]]);
            assert_eq!(actual, expected, "code-unit split {split}");
            assert!(!parser.incomplete());
            assert!(!parser.malformed());
        }
        let chunks = input.as_utf16().chunks(1).collect::<Vec<_>>();
        let (actual, parser) = parse(&chunks);
        assert_eq!(actual, expected, "one UTF-16 unit per provider delta");
        assert!(!parser.incomplete());
        assert!(!parser.malformed());
    }

    #[test]
    fn exact_utf16_repeated_and_nested_blocks_keep_the_existing_rejection_semantics() {
        let high = RuntimeString::from_utf16(vec![0xd800]);
        let low = RuntimeString::from_utf16(vec![0xdc00]);
        let repeated = RuntimeString::from("<proposed_plan>\n")
            .concat(&high)
            .concat(&"\n</proposed_plan>\nbetween\n<proposed_plan>\n".into())
            .concat(&low)
            .concat(&"\n</proposed_plan>\nafter".into());
        let nested = RuntimeString::from("<proposed_plan>\n")
            .concat(&high)
            .concat(&"\n<proposed_plan>\n".into())
            .concat(&low)
            .concat(&"\n</proposed_plan>\n</proposed_plan>".into());
        for (input, expected, incomplete) in [
            (
                repeated,
                vec![
                    PlanSegment::Start,
                    PlanSegment::Delta(high.clone()),
                    PlanSegment::End,
                    PlanSegment::Text(
                        RuntimeString::from("between\n")
                            .concat(&low)
                            .concat(&"after".into()),
                    ),
                ],
                false,
            ),
            (
                nested,
                vec![
                    PlanSegment::Start,
                    PlanSegment::Delta(high.concat(&"\n".into()).concat(&low)),
                ],
                true,
            ),
        ] {
            for split in 0..=input.len_utf16() {
                let (actual, parser) =
                    parse(&[&input.as_utf16()[..split], &input.as_utf16()[split..]]);
                assert_eq!(actual, expected, "code-unit split {split}");
                assert!(parser.malformed());
                assert_eq!(parser.incomplete(), incomplete);
            }
        }
    }

    #[test]
    fn interruption_at_every_utf16_unit_keeps_exact_unfinished_body_and_never_completes() {
        let body = RuntimeString::from_utf16(vec![0xd800, 0x0078, 0xdc00, 0xd83e, 0xdd80]);
        let input = RuntimeString::from("<proposed_plan>\n")
            .concat(&body)
            .concat(&"\r\n</proposed_plan>".into());
        let body_start = OPEN.len() + 1;
        for cut in 0..input.len_utf16() {
            let (segments, parser) = parse(&[&input.as_utf16()[..cut]]);
            assert!(!segments.contains(&PlanSegment::End), "cut {cut}");
            assert_eq!(parser.incomplete(), cut >= OPEN.len(), "cut {cut}");
            assert!(!parser.malformed());
            if cut > body_start {
                let actual = segments
                    .iter()
                    .filter_map(|segment| match segment {
                        PlanSegment::Delta(text) => Some(text.as_utf16()),
                        _ => None,
                    })
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>();
                assert_eq!(actual, input.as_utf16()[body_start..cut], "cut {cut}");
            }
        }
    }

    #[test]
    fn exact_utf16_message_boundaries_preserve_units_and_reject_unfinished_proposals() {
        let mut parser = ProposedPlanParser::default();
        let mut output = parser.push_runtime(&RuntimeString::from_utf16(vec![0xd800, 0x000d]));
        output.extend(parser.finish_message());
        output.extend(
            parser.push_runtime(
                &RuntimeString::from("<proposed_plan>\n")
                    .concat(&RuntimeString::from_utf16(vec![0xdc00]))
                    .concat(&"\n</proposed_plan>".into()),
            ),
        );
        output.extend(parser.finish_message());
        assert_eq!(
            coalesce(output),
            vec![
                PlanSegment::Text(RuntimeString::from_utf16(vec![0xd800, 0x000d])),
                PlanSegment::Start,
                PlanSegment::Delta(RuntimeString::from_utf16(vec![0xdc00])),
                PlanSegment::End
            ]
        );
        assert!(!parser.malformed());
        assert!(!parser.incomplete());

        let mut parser = ProposedPlanParser::default();
        let mut output = parser.push_runtime(
            &RuntimeString::from("<proposed_plan>\n")
                .concat(&RuntimeString::from_utf16(vec![0xd800])),
        );
        output.extend(parser.finish_message());
        output.extend(parser.push_runtime(&RuntimeString::from("\n</proposed_plan>")));
        output.extend(parser.finish());
        assert!(!output.contains(&PlanSegment::End));
        assert!(parser.malformed());
        assert!(parser.incomplete());
    }

    #[test]
    fn long_non_scalar_lines_coalesce_once_per_push_without_retaining_the_body() {
        let text = RuntimeString::from_utf16(vec![0xd800; 128 * 1024]);
        let mut parser = ProposedPlanParser::default();
        assert_eq!(
            parser.push_runtime(&text),
            vec![PlanSegment::Text(text.clone())]
        );
        assert!(parser.candidate.is_empty());
        assert_eq!(
            parser.push("\n<proposed_plan>\n"),
            vec![PlanSegment::Text("\n".into()), PlanSegment::Start]
        );
        assert_eq!(parser.push_runtime(&text), vec![PlanSegment::Delta(text)]);
        assert!(parser.candidate.is_empty());
        assert_eq!(parser.push("\n</proposed_plan>"), Vec::new());
        assert_eq!(parser.finish(), vec![PlanSegment::End]);
    }
}
