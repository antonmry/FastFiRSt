#[cfg(not(feature = "datafusion"))]
fn main() {
    eprintln!(
        "This example requires the `datafusion` feature. Run:\n  cargo run -p flash-df --example flash_cli --features datafusion -- ..."
    );
}

#[cfg(feature = "datafusion")]
fn main() -> anyhow::Result<()> {
    use std::env;
    use std::fs;
    use std::path::PathBuf;

    use flash_df::{FlashDistributedJob, FlashJobConfig};
    use flash_lib::CombineParams;

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: cargo run -p flash-df --example flash_cli --features datafusion -- \
             <forward.fq> <reverse.fq> [output_dir] [output_prefix] [--batch-size N] [--workers M]\n\
             Example: cargo run -p flash-df --example flash_cli --features datafusion -- \
             input1.fq input2.fq ./out flash --batch-size 4096 --workers 8"
        );
        std::process::exit(2);
    }

    let forward = PathBuf::from(&args[1]);
    let reverse = PathBuf::from(&args[2]);
    let output_dir = args
        .get(3)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let output_prefix = args.get(4).cloned().unwrap_or_else(|| "flash".to_string());

    let mut batch_size = None;
    let mut workers = None;

    let mut idx = 5;
    while idx < args.len() {
        match args[idx].as_str() {
            "--batch-size" => {
                idx += 1;
                let value = args.get(idx).expect("--batch-size requires a value");
                batch_size = Some(value.parse::<usize>().expect("invalid batch size"));
            }
            "--workers" => {
                idx += 1;
                let value = args.get(idx).expect("--workers requires a value");
                workers = Some(value.parse::<usize>().expect("invalid worker count"));
            }
            other => {
                eprintln!("Unknown argument: {other}");
                std::process::exit(2);
            }
        }
        idx += 1;
    }

    fs::create_dir_all(&output_dir)?;

    let mut config = FlashJobConfig::new(&forward, &reverse, &output_dir, &output_prefix);
    if let Some(size) = batch_size {
        config = config.with_batch_size(size);
    }
    if let Some(count) = workers {
        config = config.with_worker_threads(count);
    }

    let job = FlashDistributedJob::new(config, CombineParams::default());

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        job.execute_datafusion().await?;
        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}
