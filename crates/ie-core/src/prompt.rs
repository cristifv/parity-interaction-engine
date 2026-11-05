//! `say`'s pre-rendered prompts (PLAN question 7): a `say` plays the tenant prompt
//! `say-<first 24 hex digits of sha256(language "\n" voice "\n" text)>`, which the rendering
//! step (later; the demo writes tones) puts in parity-ms's prompt directory under the tenant.

use sha2::{Digest, Sha256};

/// The prompt id of a `say` (a valid TC `prompt_id` and parity-ms file name).
pub fn say_prompt_id(language: &str, voice: &str, text: &str) -> String {
    let mut h = Sha256::new();
    h.update(language.as_bytes());
    h.update(b"\n");
    h.update(voice.as_bytes());
    h.update(b"\n");
    h.update(text.as_bytes());
    let d = h.finalize();
    let hex: String = d.iter().take(12).map(|b| format!("{b:02x}")).collect();
    format!("say-{hex}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_id_is_stable_and_depends_on_every_part() {
        let a = say_prompt_id("en-GB", "", "Welcome to Acme.");
        assert_eq!(a, say_prompt_id("en-GB", "", "Welcome to Acme."));
        assert_eq!(a.len(), 4 + 24);
        assert_ne!(a, say_prompt_id("en-US", "", "Welcome to Acme."));
        assert_ne!(a, say_prompt_id("en-GB", "x", "Welcome to Acme."));
        // What the demo's renderer computes: "say-" + sha256(b"en-GB\n\nWelcome to Acme.")[:24].
        assert_eq!(a, "say-5d442aa9185a6ec5be2c5e38");
    }
}
