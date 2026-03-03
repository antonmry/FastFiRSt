pub mod io;
pub mod motif;
pub mod scanner;
pub mod stats;

pub use scanner::RepeatIndex;

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct MotifConfig {
    pub min_size: usize,
    pub max_size: usize,
    pub min_repeat_length: usize,
    pub custom_motifs: Option<Vec<String>>,
}

impl Default for MotifConfig {
    fn default() -> Self {
        Self {
            min_size: 1,
            max_size: 6,
            min_repeat_length: 12,
            custom_motifs: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScanConfig {
    pub min_motif_size: usize,
    pub max_motif_size: usize,
    pub min_repeat_length: usize,
    pub min_seq_len: usize,
    pub max_seq_len: usize,
    pub min_units: HashMap<usize, usize>,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            min_motif_size: 1,
            max_motif_size: 6,
            min_repeat_length: 12,
            min_seq_len: 0,
            max_seq_len: usize::MAX,
            min_units: HashMap::new(),
        }
    }
}
