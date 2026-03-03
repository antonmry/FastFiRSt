use std::collections::{HashMap, HashSet};

use anyhow::{Result, ensure};

use crate::MotifConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Strand {
    Forward,
    Reverse,
}

impl Strand {
    pub fn as_char(self) -> char {
        match self {
            Strand::Forward => '+',
            Strand::Reverse => '-',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepeatInfo {
    pub canonical: String,
    pub motif_len: usize,
    pub strand: Strand,
}

pub fn reverse_complement(seq: &str) -> String {
    seq.as_bytes()
        .iter()
        .rev()
        .map(|base| match base.to_ascii_uppercase() {
            b'A' => 'T',
            b'C' => 'G',
            b'G' => 'C',
            b'T' => 'A',
            _ => 'N',
        })
        .collect()
}

pub fn get_cycles(motif: &str) -> Vec<String> {
    if motif.is_empty() {
        return Vec::new();
    }

    let bytes = motif.as_bytes();
    let mut cycles = Vec::with_capacity(bytes.len());
    for offset in 0..bytes.len() {
        let mut cycle = String::with_capacity(bytes.len());
        for idx in 0..bytes.len() {
            cycle.push(bytes[(offset + idx) % bytes.len()] as char);
        }
        cycles.push(cycle);
    }
    cycles
}

pub fn is_atomic(motif: &str) -> bool {
    let n = motif.len();
    if n <= 1 {
        return true;
    }

    for period in 1..=(n / 2) {
        if !n.is_multiple_of(period) {
            continue;
        }
        let unit = &motif[0..period];
        if unit.repeat(n / period) == motif {
            return false;
        }
    }

    true
}

pub fn expand_repeat(motif: &str, target_len: usize) -> String {
    if motif.is_empty() || target_len == 0 {
        return String::new();
    }

    let mut out = String::with_capacity(target_len);
    while out.len() < target_len {
        out.push_str(motif);
    }
    out.truncate(target_len);
    out
}

pub fn generate_repeats(min_size: usize, max_size: usize) -> Vec<String> {
    let mut canonical = HashSet::new();

    for size in min_size..=max_size {
        generate_size(size, String::with_capacity(size), &mut |motif| {
            if !is_atomic(motif) {
                return;
            }
            let rep = canonical_motif(motif);
            canonical.insert(rep);
        });
    }

    let mut motifs = canonical.into_iter().collect::<Vec<_>>();
    motifs.sort();
    motifs
}

pub fn canonical_motif(motif: &str) -> String {
    let mut all = get_cycles(motif);
    let rc = reverse_complement(motif);
    all.extend(get_cycles(&rc));
    all.into_iter().min().unwrap_or_default()
}

pub fn build_rep_set(config: &MotifConfig) -> Result<HashMap<String, RepeatInfo>> {
    ensure!(config.min_size >= 1, "min motif size must be >= 1");
    ensure!(
        config.max_size >= config.min_size,
        "max motif size must be >= min motif size"
    );
    ensure!(
        config.min_repeat_length >= 1,
        "min repeat length must be >= 1"
    );

    let motifs = if let Some(custom) = &config.custom_motifs {
        let mut uniq = HashSet::new();
        for motif in custom {
            let upper = motif.trim().to_ascii_uppercase();
            if upper.is_empty() {
                continue;
            }
            ensure!(
                upper
                    .bytes()
                    .all(|b| matches!(b, b'A' | b'C' | b'G' | b'T')),
                "custom motif contains non-ACGT characters: {upper}"
            );
            ensure!(
                upper.len() >= config.min_size && upper.len() <= config.max_size,
                "custom motif size out of range [{}..={}]: {upper}",
                config.min_size,
                config.max_size
            );
            uniq.insert(canonical_motif(&upper));
        }
        let mut motifs = uniq.into_iter().collect::<Vec<_>>();
        motifs.sort();
        motifs
    } else {
        generate_repeats(config.min_size, config.max_size)
    };

    let mut reps = HashMap::new();
    for motif in motifs {
        add_motif_variants(&mut reps, &motif, config.min_repeat_length);
    }

    Ok(reps)
}

fn add_motif_variants(reps: &mut HashMap<String, RepeatInfo>, motif: &str, target_len: usize) {
    for cycle in get_cycles(motif) {
        let expanded = expand_repeat(&cycle, target_len);
        reps.entry(expanded).or_insert_with(|| RepeatInfo {
            canonical: motif.to_string(),
            motif_len: motif.len(),
            strand: Strand::Forward,
        });
    }

    let rc = reverse_complement(motif);
    for cycle in get_cycles(&rc) {
        let expanded = expand_repeat(&cycle, target_len);
        reps.entry(expanded).or_insert_with(|| RepeatInfo {
            canonical: motif.to_string(),
            motif_len: motif.len(),
            strand: Strand::Reverse,
        });
    }
}

fn generate_size(size: usize, prefix: String, callback: &mut impl FnMut(&str)) {
    if prefix.len() == size {
        callback(&prefix);
        return;
    }

    for base in ["A", "C", "G", "T"] {
        let mut next = prefix.clone();
        next.push_str(base);
        generate_size(size, next, callback);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_complement_works() {
        assert_eq!(reverse_complement("ACGT"), "ACGT");
        assert_eq!(reverse_complement("AAGT"), "ACTT");
    }

    #[test]
    fn atomicity_detection_works() {
        assert!(is_atomic("AC"));
        assert!(!is_atomic("ATAT"));
        assert!(!is_atomic("AAAA"));
    }

    #[test]
    fn repeat_generation_is_deduplicated() {
        let repeats = generate_repeats(1, 2);
        assert!(repeats.contains(&"A".to_string()));
        assert!(repeats.contains(&"AC".to_string()));
        assert!(!repeats.contains(&"TA".to_string()));
    }

    #[test]
    fn rep_set_contains_expanded_key() {
        let cfg = MotifConfig::default();
        let map = build_rep_set(&cfg).unwrap();
        assert!(map.contains_key("AAAAAAAAAAAA"));
    }
}
