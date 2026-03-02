use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser};
use flash_lib::{CombineParams, DEFAULT_BATCH_SIZE, DEFAULT_NUM_THREADS, merge_fastq_files};

fn main() -> Result<()> {
    let cli = Cli::parse();
    let params = cli.params.into_params();

    if cli.parallel {
        #[cfg(feature = "parallel")]
        {
            return flash_lib::merge_fastq_files_parallel(
                cli.forward,
                cli.reverse,
                cli.output_dir,
                &cli.output_prefix,
                &params,
                cli.batch_size,
                cli.threads,
            );
        }

        #[cfg(not(feature = "parallel"))]
        {
            anyhow::bail!(
                "--parallel requested, but this binary was built without the 'parallel' feature"
            )
        }
    }

    merge_fastq_files(
        cli.forward,
        cli.reverse,
        cli.output_dir,
        &cli.output_prefix,
        &params,
    )
}

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Rust port of the FLASH paired-end read merger."
)]
struct Cli {
    /// Forward input FASTQ file
    #[arg(value_name = "READ1")]
    forward: PathBuf,

    /// Reverse input FASTQ file
    #[arg(value_name = "READ2")]
    reverse: PathBuf,

    #[command(flatten)]
    params: CombineArgs,

    /// Output directory (defaults to current working directory)
    #[arg(long, value_name = "DIR", default_value = ".")]
    output_dir: PathBuf,

    /// Output prefix (defaults to `out`, matching FLASH)
    #[arg(long, value_name = "PREFIX", default_value = "out")]
    output_prefix: String,

    /// Enable batch-parallel pair processing (requires build with --features parallel)
    #[arg(long, default_value_t = false)]
    parallel: bool,

    /// Number of read pairs per parallel batch
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,

    /// Number of compute threads for parallel mode
    #[arg(long, default_value_t = DEFAULT_NUM_THREADS)]
    threads: usize,
}

#[derive(Args, Debug, Clone)]
struct CombineArgs {
    /// Minimum overlap length
    #[arg(short = 'm', long = "min-overlap", default_value_t = 10)]
    min_overlap: usize,

    /// Maximum overlap length used for scoring
    #[arg(short = 'M', long = "max-overlap", default_value_t = 65)]
    max_overlap: usize,

    /// Maximum allowed mismatch density
    #[arg(short = 'x', long = "max-mismatch-density", default_value_t = 0.25)]
    max_mismatch_density: f32,

    /// Cap mismatch qualities at 2 (legacy behaviour)
    #[arg(long = "cap-mismatch-quals", default_value_t = false)]
    cap_mismatch_quals: bool,

    /// Allow "outie" orientation mergers
    #[arg(short = 'O', long = "allow-outies", default_value_t = false)]
    allow_outies: bool,

    /// Lowercase the non-overlapped overhangs in the merged read
    #[arg(long = "lowercase-overhang", default_value_t = false)]
    lowercase_overhang: bool,

    /// PHRED offset (usually 33 or 64)
    #[arg(short = 'p', long = "phred-offset", default_value_t = 33)]
    phred_offset: u8,
}

impl CombineArgs {
    fn into_params(self) -> CombineParams {
        CombineParams {
            min_overlap: self.min_overlap,
            max_overlap: self.max_overlap,
            max_mismatch_density: self.max_mismatch_density,
            cap_mismatch_quals: self.cap_mismatch_quals,
            allow_outies: self.allow_outies,
            lowercase_overhang: self.lowercase_overhang,
            phred_offset: self.phred_offset,
        }
    }
}
