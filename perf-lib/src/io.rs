use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;

use anyhow::{Context, Result};
use flate2::read::GzDecoder;

const READ_BUF: usize = 256 * 1024; // 256 KB
const WRITE_BUF: usize = 1024 * 1024; // 1 MB

pub fn open_maybe_gzip(path: &Path) -> Result<Box<dyn BufRead>> {
    if is_gzip(path)? {
        let file = File::open(path)
            .with_context(|| format!("failed to open compressed input {}", path.display()))?;
        let decoder = GzDecoder::new(file);
        Ok(Box::new(BufReader::with_capacity(READ_BUF, decoder)))
    } else {
        let file =
            File::open(path).with_context(|| format!("failed to open input {}", path.display()))?;
        advise_sequential(&file);
        Ok(Box::new(BufReader::with_capacity(READ_BUF, file)))
    }
}

pub fn create_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = File::create(path)
        .with_context(|| format!("failed to create output {}", path.display()))?;
    Ok(BufWriter::with_capacity(WRITE_BUF, file))
}

pub fn write_fasta_footer(
    writer: &mut impl Write,
    filename: &str,
    total_seqs: usize,
    total_bases: u64,
    gc_pct: f64,
) -> Result<()> {
    writeln!(writer, "# file: {filename}")?;
    writeln!(writer, "# total_sequences: {total_seqs}")?;
    writeln!(writer, "# total_bases: {total_bases}")?;
    writeln!(writer, "# gc_pct: {gc_pct:.2}")?;
    Ok(())
}

fn is_gzip(path: &Path) -> Result<bool> {
    if path
        .extension()
        .map(|ext| ext.eq_ignore_ascii_case("gz"))
        .unwrap_or(false)
    {
        return Ok(true);
    }

    let mut file = File::open(path).with_context(|| {
        format!(
            "failed to open input for format detection {}",
            path.display()
        )
    })?;
    let mut magic = [0u8; 2];
    let read = file.read(&mut magic)?;
    Ok(read == 2 && magic == [0x1f, 0x8b])
}

/// Hint the kernel to read this file sequentially (Linux only).
#[cfg(target_os = "linux")]
fn advise_sequential(file: &File) {
    use std::os::unix::io::AsRawFd;
    // POSIX_FADV_SEQUENTIAL = 2
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
    }
}

#[cfg(not(target_os = "linux"))]
fn advise_sequential(_file: &File) {}
