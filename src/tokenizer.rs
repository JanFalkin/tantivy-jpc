use std::iter::Peekable;
use std::str::CharIndices;

use super::{Token, TokenStream, Tokenizer};

/// Tokenize the text by splitting on whitespaces, punctuation, and camel case and digit transitions.
#[derive(Clone)]
pub struct CamelCaseDigitTokenizer;

pub struct CamelCaseDigitTokenStream<'a> {
    text: &'a str,
    // peekable char indices
    chars: Peekable<CharIndices<'a>>,
    token: Token,
}

impl Tokenizer for CamelCaseDigitTokenizer {
    type TokenStream<'a> = CamelCaseDigitTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> CamelCaseDigitTokenStream<'a> {
        CamelCaseDigitTokenStream {
            text,
            chars: text.char_indices().peekable(),
            token: Token::default(),
        }
    }
}

impl<'a> CamelCaseDigitTokenStream<'a> {
    // Search for the end of the current token, considering camel case, digit boundaries,
    // and transitions between letters and digits.
    fn search_token_end(&mut self, start_offset: usize) -> usize {
        let first_char = self.text[start_offset..].chars().next();
        let mut prev_char_is_digit = first_char.map_or(false, |ch| ch.is_ascii_digit());
        let mut prev_char_is_lowercase = first_char.map_or(false, |ch| ch.is_lowercase());

        while let Some(&(offset, c)) = self.chars.peek() {
            let is_transition = if c.is_uppercase() {
                prev_char_is_lowercase || prev_char_is_digit
            } else if c.is_ascii_digit() {
                !prev_char_is_digit
            } else {
                false
            };

            if !c.is_alphanumeric() {
                self.chars.next(); // Advance iterator when character is not alphanumeric
                return offset;
            } else if is_transition {
                return offset; // Do not advance iterator on transition
            } else {
                self.chars.next(); // Advance iterator for normal characters
            }

            prev_char_is_digit = c.is_ascii_digit();
            prev_char_is_lowercase = c.is_lowercase();
        }

        self.text.len()
    }
}

impl<'a> TokenStream for CamelCaseDigitTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        while let Some((offset_from, c)) = self.chars.next() {
            if c.is_alphanumeric() {
                let offset_to = self.search_token_end(offset_from);
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;
                self.token.text.push_str(&self.text[offset_from..offset_to]);
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}
