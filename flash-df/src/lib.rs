//! DataFusion and Ballista integration layer for the FLASH port.
//!
//! The goal of this crate is to expose helpers that register the FASTQ inputs
//! handled by `flash-lib` as DataFusion data sources and build the logical plan
//! necessary to reproduce FLASH's merging behaviour.  It also sketches the
//! entry points needed to submit the job to a Ballista cluster.  The heavy
//! lifting is still carried out by `flash-lib`; this crate focuses on the
//! distributed execution wiring.

use anyhow::Result;
use flash_lib::{CombineParams, merge_fastq_files};

#[cfg(feature = "datafusion")]
use anyhow::anyhow;
#[cfg(feature = "datafusion")]
use flash_lib::FastqPairReader;
use std::path::{Path, PathBuf};

/// Configuration for a FLASH job, regardless of which execution backend is
/// used.
#[derive(Debug, Clone)]
pub struct FlashJobConfig {
    /// Forward FASTQ file.
    pub forward: PathBuf,
    /// Reverse FASTQ file.
    pub reverse: PathBuf,
    /// Output directory to receive the three FASTQ artefacts.
    pub output_dir: PathBuf,
    /// Output prefix (defaults to `out` in the CLI).
    pub output_prefix: String,
}

impl FlashJobConfig {
    /// Build a new configuration description.
    pub fn new(
        forward: impl AsRef<Path>,
        reverse: impl AsRef<Path>,
        output_dir: impl AsRef<Path>,
        output_prefix: impl Into<String>,
    ) -> Self {
        Self {
            forward: forward.as_ref().to_path_buf(),
            reverse: reverse.as_ref().to_path_buf(),
            output_dir: output_dir.as_ref().to_path_buf(),
            output_prefix: output_prefix.into(),
        }
    }
}

/// Wrapper that couples job configuration with the merge parameters.
#[derive(Debug, Clone)]
pub struct FlashDistributedJob {
    config: FlashJobConfig,
    params: CombineParams,
}

impl FlashDistributedJob {
    /// Compose a distributed job description.
    pub fn new(config: FlashJobConfig, params: CombineParams) -> Self {
        Self { config, params }
    }

    /// Borrow the configuration component.
    pub fn config(&self) -> &FlashJobConfig {
        &self.config
    }

    /// Borrow the merge parameters.
    pub fn params(&self) -> &CombineParams {
        &self.params
    }

    /// Execute the job locally using the reference implementation in
    /// `flash-lib`.  This is handy for testing the distributed pipeline logic.
    pub fn execute_local(&self) -> Result<()> {
        merge_fastq_files(
            &self.config.forward,
            &self.config.reverse,
            &self.config.output_dir,
            &self.config.output_prefix,
            &self.params,
        )
    }
}

#[cfg(feature = "datafusion")]
use std::any::Any;
#[cfg(feature = "datafusion")]
use std::sync::Arc;

#[cfg(feature = "datafusion")]
use async_trait::async_trait;
#[cfg(feature = "datafusion")]
use datafusion::arrow::array::{ArrayRef, StringBuilder};
#[cfg(feature = "datafusion")]
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
#[cfg(feature = "datafusion")]
use datafusion::arrow::record_batch::RecordBatch;
#[cfg(feature = "datafusion")]
use datafusion::catalog::Session;
#[cfg(feature = "datafusion")]
use datafusion::datasource::{TableProvider, TableType};
#[cfg(feature = "datafusion")]
use datafusion::error::{DataFusionError, Result as DFResult};
#[cfg(feature = "datafusion")]
use datafusion::execution::context::SessionContext;
#[cfg(feature = "datafusion")]
use datafusion::logical_expr::{Expr, LogicalPlan};
#[cfg(feature = "datafusion")]
use datafusion::physical_plan::{ExecutionPlan, memory::MemoryExec};
#[cfg(feature = "datafusion")]
use datafusion::prelude::SessionConfig;

#[cfg(feature = "datafusion")]
const FASTQ_TABLE_NAME: &str = "flash_pairs";

#[cfg(feature = "datafusion")]
fn to_df_error(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

#[cfg(feature = "datafusion")]
pub struct FastqTableProvider {
    forward: PathBuf,
    reverse: PathBuf,
    phred_offset: u8,
    schema: SchemaRef,
}

#[cfg(feature = "datafusion")]
impl FastqTableProvider {
    pub fn new(forward: PathBuf, reverse: PathBuf, phred_offset: u8) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, false),
            Field::new("seq1", DataType::Utf8, false),
            Field::new("qual1", DataType::Utf8, false),
            Field::new("tag2", DataType::Utf8, false),
            Field::new("seq2", DataType::Utf8, false),
            Field::new("qual2", DataType::Utf8, false),
        ]));
        Self {
            forward,
            reverse,
            phred_offset,
            schema,
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(feature = "datafusion")]
#[async_trait]
impl TableProvider for FastqTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut pair_reader =
            FastqPairReader::from_paths(&self.forward, &self.reverse, self.phred_offset)
                .map_err(to_df_error)?;

        let mut tag1 = StringBuilder::new();
        let mut seq1 = StringBuilder::new();
        let mut qual1 = StringBuilder::new();
        let mut tag2 = StringBuilder::new();
        let mut seq2 = StringBuilder::new();
        let mut qual2 = StringBuilder::new();

        let mut rows = 0usize;
        loop {
            if let Some(max) = limit {
                if rows >= max {
                    break;
                }
            }

            match pair_reader.next_pair().map_err(to_df_error)? {
                Some((r1, r2)) => {
                    let seq1_str = r1.seq_string();
                    let qual1_str = r1.qual_string(self.phred_offset);
                    let seq2_str = r2.seq_string();
                    let qual2_str = r2.qual_string(self.phred_offset);

                    tag1.append_value(r1.tag());
                    seq1.append_value(&seq1_str);
                    qual1.append_value(&qual1_str);

                    tag2.append_value(r2.tag());
                    seq2.append_value(&seq2_str);
                    qual2.append_value(&qual2_str);
                    rows += 1;
                }
                None => break,
            }
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(tag1.finish()),
            Arc::new(seq1.finish()),
            Arc::new(qual1.finish()),
            Arc::new(tag2.finish()),
            Arc::new(seq2.finish()),
            Arc::new(qual2.finish()),
        ];

        let schema = self.schema();
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DataFusionError::ArrowError(e, None))?;

        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

#[cfg(feature = "datafusion")]
impl FlashDistributedJob {
    /// Construct a `SessionContext` configured for FLASH workloads.
    pub async fn session_context(&self) -> Result<SessionContext> {
        let mut cfg = SessionConfig::new();
        cfg = cfg.with_target_partitions(1);
        Ok(SessionContext::new_with_config(cfg))
    }

    pub fn fastq_table_provider(&self) -> FastqTableProvider {
        FastqTableProvider::new(
            self.config.forward.clone(),
            self.config.reverse.clone(),
            self.params.phred_offset,
        )
    }

    /// Register the FASTQ sources backing this job.
    pub async fn register_fastq_sources(&self, ctx: &SessionContext) -> Result<()> {
        let provider = Arc::new(self.fastq_table_provider());
        ctx.register_table(FASTQ_TABLE_NAME, provider)
            .map_err(|e| anyhow!(e.to_string()))?;
        Ok(())
    }

    /// Build the logical plan that mirrors FLASH's merge stages. For now this
    /// simply returns the plan that scans the registered table.
    pub async fn build_logical_plan(&self, ctx: &SessionContext) -> Result<LogicalPlan> {
        let df = ctx
            .table(FASTQ_TABLE_NAME)
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        let plan = df
            .into_optimized_plan()
            .map_err(|e| anyhow!(e.to_string()))?;
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_config_builder_roundtrips() {
        let cfg = FlashJobConfig::new("a", "b", "c", "prefix");
        let job = FlashDistributedJob::new(cfg.clone(), CombineParams::default());
        assert_eq!(job.config().forward, cfg.forward);
        assert_eq!(job.config().output_prefix, cfg.output_prefix);
    }
}
