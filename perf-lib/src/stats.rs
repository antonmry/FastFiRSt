use std::collections::{HashMap, HashSet};
use std::io::Write;

use anyhow::Result;

use crate::motif::{RepeatInfo, Strand};
use crate::scanner::SsrMatch;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepeatClass {
    pub canonical: String,
    pub motif_len: usize,
    pub strand: Strand,
}

impl RepeatClass {
    pub fn from_info(info: &RepeatInfo) -> Self {
        Self {
            canonical: info.canonical.clone(),
            motif_len: info.motif_len,
            strand: info.strand,
        }
    }

    pub fn from_match(m: &SsrMatch) -> Self {
        Self {
            canonical: m.canonical.clone(),
            motif_len: m.canonical.len(),
            strand: m.strand,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ClassStats {
    pub instances: u64,
    pub reads: u64,
    pub covered_bases: u64,
}

#[derive(Debug, Clone, Default)]
pub struct FastqStats {
    pub total_reads: u64,
    pub total_bases: u64,
    pub classes: HashMap<RepeatClass, ClassStats>,
}

impl FastqStats {
    pub fn observe_read(&mut self, read_len: usize, matches: &[SsrMatch]) {
        self.total_reads += 1;
        self.total_bases += read_len as u64;

        let mut seen_in_read = HashSet::new();
        for m in matches {
            let class = RepeatClass::from_match(m);
            let entry = self.classes.entry(class.clone()).or_default();
            entry.instances += 1;
            entry.covered_bases += m.length as u64;
            if seen_in_read.insert(class) {
                entry.reads += 1;
            }
        }
    }

    pub fn write_tsv(&self, writer: &mut impl Write) -> Result<()> {
        writeln!(
            writer,
            "class\tstrand\tmotif_len\tinstances\treads\tcovered_bases\tinstances_per_million_reads\tbases_per_million_bases"
        )?;

        let mut rows: Vec<_> = self.classes.iter().collect();
        rows.sort_by(|(a, _), (b, _)| {
            a.motif_len
                .cmp(&b.motif_len)
                .then(a.canonical.cmp(&b.canonical))
                .then(a.strand.as_char().cmp(&b.strand.as_char()))
        });

        for (class, stat) in rows {
            let inst_per_million_reads = if self.total_reads > 0 {
                stat.instances as f64 * 1_000_000.0 / self.total_reads as f64
            } else {
                0.0
            };

            let bases_per_million_bases = if self.total_bases > 0 {
                stat.covered_bases as f64 * 1_000_000.0 / self.total_bases as f64
            } else {
                0.0
            };

            writeln!(
                writer,
                "{}\t{}\t{}\t{}\t{}\t{}\t{:.4}\t{:.4}",
                class.canonical,
                class.strand.as_char(),
                class.motif_len,
                stat.instances,
                stat.reads,
                stat.covered_bases,
                inst_per_million_reads,
                bases_per_million_bases
            )?;
        }

        writeln!(writer, "# total_reads\t{}", self.total_reads)?;
        writeln!(writer, "# total_bases\t{}", self.total_bases)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::motif::Strand;
    use crate::scanner::SsrMatch;

    #[test]
    fn aggregates_stats() {
        let mut stats = FastqStats::default();
        let matches = vec![SsrMatch {
            seq_name: "r1".to_string(),
            start: 0,
            end: 12,
            motif: "A".to_string(),
            length: 12,
            strand: Strand::Forward,
            num_units: 12,
            canonical: "A".to_string(),
        }];

        stats.observe_read(20, &matches);

        assert_eq!(stats.total_reads, 1);
        assert_eq!(stats.total_bases, 20);
        assert_eq!(stats.classes.len(), 1);
    }
}
