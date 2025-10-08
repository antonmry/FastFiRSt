#[cfg(not(feature = "datafusion"))]
fn main() {
    eprintln!(
        "This example requires the `datafusion` feature. Run:\n  cargo run -p flash-df --example query --features datafusion -- ..."
    );
}

#[cfg(feature = "datafusion")]
fn main() -> anyhow::Result<()> {
    use std::env;
    use std::path::PathBuf;

    use flash_df::{FlashDistributedJob, FlashJobConfig};
    use flash_lib::CombineParams;

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: cargo run -p flash-df --example query --features datafusion -- <forward.fq> <reverse.fq> [SQL]\n\
             Example: cargo run -p flash-df --example query --features datafusion -- input1.fq input2.fq \"SELECT tag1, seq1 FROM flash_pairs LIMIT 5\""
        );
        std::process::exit(2);
    }

    let forward = PathBuf::from(&args[1]);
    let reverse = PathBuf::from(&args[2]);
    let sql = if args.len() > 3 {
        args[3..].join(" ")
    } else {
        "SELECT tag1, seq1, tag2, seq2 FROM flash_pairs LIMIT 10".to_string()
    };

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let job = FlashDistributedJob::new(
            FlashJobConfig::new(&forward, &reverse, std::env::temp_dir(), "df_preview"),
            CombineParams::default(),
        );

        let ctx = job.session_context().await?;
        job.register_fastq_sources(&ctx).await?;

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        for batch in batches {
            println!("{batch:?}");
        }
        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}
