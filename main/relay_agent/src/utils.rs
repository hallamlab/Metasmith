use std::time::SystemTime;
use rand::{SeedableRng, rngs::StdRng, Rng};
use std::collections::HashSet;

// =========================================================================
// RUST EQUIVALENT OF _ASCII_VOCAB_62
// =========================================================================

// Define the vocabulary as a static constant string, or an array of characters.
// Using a static string slice for memory efficiency.
const VOCABULARY: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

// =========================================================================
// RUST EQUIVALENT OF class KeyGenerator
// =========================================================================

pub struct KeyGenerator {
    // We use StdRng which is a good general-purpose PRNG and can be seeded.
    generator: StdRng,
}

impl KeyGenerator {
    /// Equivalent to def __init__(self, seed=None)
    pub fn new(seed: Option<u64>) -> Self {
        let generator = match seed {
            // Seed the generator using the provided seed
            Some(s) => StdRng::seed_from_u64(s),
            // Use non-deterministic entropy if no seed is provided (like default_rng)
            None => StdRng::from_entropy(), 
        };
        KeyGenerator { generator }
    }

    /// Equivalent to GenerateUID(self, l:int=8, blacklist: set[str]=set())
    // Note: We use l=8 as a Rust default argument by wrapping this method.
    fn generate_uid_impl(&mut self, length: usize, blacklist: &HashSet<String>) -> String {
        let vocab_chars: Vec<char> = VOCABULARY.chars().collect();
        
        loop {
            // 1. Generate random indices / Choose random characters
            // The choose_multiple method selects a number of random elements from an iterator.
            let rng = &mut self.generator; // get mutable reference
            let key_chars: String = rng
                .sample_iter(&rand::distributions::Uniform::new(0, vocab_chars.len()))
                .map(|i| vocab_chars[i])
                .take(length)
                .collect();
            
            // 2. key: str|None = None / while key is None or key in blacklist:
            if !blacklist.contains(&key_chars) {
                return key_chars;
            }
            // If the generated key is in the blacklist, the loop repeats.
        }
    }

    // --- Public Overloads for Default Arguments ---

    /// Public wrapper for generating a UID with default length (8) and no blacklist
    pub fn generate_uid(&mut self) -> String {
        self.generate_uid_impl(8, &HashSet::new())
    }

    // /// Public wrapper for generating a UID with specified length and no blacklist
    // pub fn generate_uid_len(&mut self, length: usize) -> String {
    //     self.generate_uid_impl(length, &HashSet::new())
    // }
    
    // /// Public wrapper for generating a UID with specified length and blacklist
    // pub fn generate_uid_full(&mut self, length: usize, blacklist: &HashSet<String>) -> String {
    //     self.generate_uid_impl(length, blacklist)
    // }
}

// A simple unique ID generator (replace with a proper UUID/ULID generator for production)
pub fn generate_id() -> String {
    // Initialize a generator (with non-deterministic seed)
    let mut generator = KeyGenerator::new(None);
    generator.generate_uid()
}

// Helper to get time in milliseconds (equivalent to CurrentTimeMillis)
pub fn current_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
