use std::time::SystemTime;
use rand::{SeedableRng, rngs::StdRng, Rng};
use std::collections::HashSet;

const VOCABULARY: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub struct KeyGenerator {

    generator: StdRng,
}

impl KeyGenerator {

    pub fn new(seed: Option<u64>) -> Self {
        let generator = match seed {

            Some(s) => StdRng::seed_from_u64(s),

            None => StdRng::from_entropy(),
        };
        KeyGenerator { generator }
    }

    fn generate_uid_impl(&mut self, length: usize, blacklist: &HashSet<String>) -> String {
        let vocab_chars: Vec<char> = VOCABULARY.chars().collect();

        loop {

            let rng = &mut self.generator;
            let key_chars: String = rng
                .sample_iter(&rand::distributions::Uniform::new(0, vocab_chars.len()))
                .map(|i| vocab_chars[i])
                .take(length)
                .collect();

            if !blacklist.contains(&key_chars) {
                return key_chars;
            }

        }
    }

    pub fn generate_uid(&mut self) -> String {
        self.generate_uid_impl(8, &HashSet::new())
    }

}

pub fn generate_id() -> String {

    let mut generator = KeyGenerator::new(None);
    generator.generate_uid()
}

pub fn current_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
