import { Alert, Button, FileInput, Group, Loader, Stack, Text } from "@mantine/core";
import { useState } from "react";
import { useSetAtom } from "jotai";
import { historyListAtom } from "./History";
import { runFlash, type FlashResult } from "../lib/flash";
import { dfCtx } from "../App";

interface DownloadTarget {
  label: string;
  filename: string;
  accessor: (result: FlashResult) => string;
}

const downloadTargets: DownloadTarget[] = [
  {
    label: "Combined Reads",
    filename: "flash.extendedFrags.fastq",
    accessor: (result) => result.combined,
  },
  {
    label: "Not Combined (forward)",
    filename: "flash.notCombined_1.fastq",
    accessor: (result) => result.notCombined1,
  },
  {
    label: "Not Combined (reverse)",
    filename: "flash.notCombined_2.fastq",
    accessor: (result) => result.notCombined2,
  },
];

export function FlashUpload() {
  const [forwardFile, setForwardFile] = useState<File | null>(null);
  const [reverseFile, setReverseFile] = useState<File | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<FlashResult | null>(null);
  const setHistoryList = useSetAtom(historyListAtom);

  const handleRun = async () => {
    if (!forwardFile || !reverseFile) {
      setError("Please select both forward and reverse FASTQ files");
      return;
    }

    setIsRunning(true);
    setError(null);

    try {
      const [forwardBuffer, reverseBuffer] = await Promise.all([
        forwardFile.arrayBuffer(),
        reverseFile.arrayBuffer(),
      ]);

      const forwardText = new TextDecoder().decode(forwardBuffer);
      const reverseText = new TextDecoder().decode(reverseBuffer);

      const forwardRecords = parseFastq(forwardText);
      const reverseRecords = parseFastq(reverseText);

      if (forwardRecords.length !== reverseRecords.length) {
        throw new Error(
          `FASTQ inputs have different number of records (${forwardRecords.length} vs ${reverseRecords.length})`,
        );
      }

      const flashResult = await runFlash(new Uint8Array(forwardBuffer), new Uint8Array(reverseBuffer));
      setResult(flashResult);

      await registerFlashTables({
        forwardRecords,
        reverseRecords,
        flashResult,
      });

      setHistoryList((history) => [
        {
          query: "-- FLASH upload --",
          result:
            "Registered tables: flash_input_pairs, flash_combined, flash_not_combined_left, flash_not_combined_right",
          isErr: false,
        },
        ...history,
      ]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setResult(null);
      setHistoryList((history) => [
        { query: "-- FLASH upload (failed) --", result: message, isErr: true },
        ...history,
      ]);
    } finally {
      setIsRunning(false);
    }
  };

  const handleDownload = (target: DownloadTarget) => {
    if (!result) return;

    const content = target.accessor(result);
    const blob = new Blob([content], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = target.filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  return (
    <Stack gap="md">
      <Text size="sm">
        Upload paired FASTQ files to run the FLASH merge algorithm directly in your browser using
        the Rust/WebAssembly port of flash-lib. The combined and not-combined outputs can be
        inspected below or queried via the registered DataFusion views.
      </Text>

      <FileInput
        label="Forward FASTQ"
        placeholder="Select forward reads (R1)"
        value={forwardFile}
        onChange={setForwardFile}
        accept=".fq,.fastq,.txt"
        withAsterisk
      />

      <FileInput
        label="Reverse FASTQ"
        placeholder="Select reverse reads (R2)"
        value={reverseFile}
        onChange={setReverseFile}
        accept=".fq,.fastq,.txt"
        withAsterisk
      />

      <Group>
        <Button onClick={handleRun} disabled={isRunning}>
          {isRunning ? (
            <Group gap="xs" align="center">
              <Loader size="xs" />
              <span>Merging…</span>
            </Group>
          ) : (
            "Run FLASH"
          )}
        </Button>
      </Group>

      {error && (
        <Alert color="red">
          {error}
        </Alert>
      )}

      {result && (
        <Stack gap="sm">
          <Text fw={600}>Outputs</Text>

          {downloadTargets.map((target) => (
            <Group key={target.label} justify="space-between" align="center">
              <Stack gap={0} className="flex-1">
                <Text fw={500}>{target.label}</Text>
                <Text size="sm" c="dimmed">
                  {formatFastqSummary(target.accessor(result))}
                </Text>
              </Stack>
              <Button variant="light" onClick={() => handleDownload(target)}>
                Download
              </Button>
            </Group>
          ))}
        </Stack>
      )}
    </Stack>
  );
}

type FastqRecord = {
  tag: string;
  seq: string;
  qual: string;
};

async function registerFlashTables(params: {
  forwardRecords: FastqRecord[];
  reverseRecords: FastqRecord[];
  flashResult: FlashResult;
}) {
  const { forwardRecords, reverseRecords, flashResult } = params;

  const combinedRecords = parseFastqSafe(flashResult.combined);
  const notLeftRecords = parseFastqSafe(flashResult.notCombined1);
  const notRightRecords = parseFastqSafe(flashResult.notCombined2);

  await createFlashView(
    "flash_input_pairs",
    ["idx", "tag1", "seq1", "qual1", "tag2", "seq2", "qual2"],
    forwardRecords.map((left, idx) => [
      idx,
      left.tag,
      left.seq,
      left.qual,
      reverseRecords[idx].tag,
      reverseRecords[idx].seq,
      reverseRecords[idx].qual,
    ]),
  );

  await createFlashView(
    "flash_combined",
    ["tag", "seq", "qual"],
    combinedRecords.map((row) => [row.tag, row.seq, row.qual]),
  );

  await createFlashView(
    "flash_not_combined_left",
    ["tag", "seq", "qual"],
    notLeftRecords.map((row) => [row.tag, row.seq, row.qual]),
  );

  await createFlashView(
    "flash_not_combined_right",
    ["tag", "seq", "qual"],
    notRightRecords.map((row) => [row.tag, row.seq, row.qual]),
  );
}

async function createFlashView(
  tableName: string,
  columns: string[],
  rows: Array<Array<string | number | null>>,
) {
  await dropRelations(tableName);

  if (rows.length === 0) {
    const selectList = columns
      .map((name) => `CAST(NULL AS TEXT) AS ${name}`)
      .join(", ");
    await dfCtx.execute_sql(
      `CREATE VIEW ${tableName} AS SELECT ${selectList} WHERE 1 = 0`,
    );
    return;
  }

  const columnList = columns.join(", ");
  const valuesSql = rows
    .map((row) => `(${row.map(sqlLiteral).join(", ")})`)
    .join(",\n    ");

  await dfCtx.execute_sql(
    `CREATE VIEW ${tableName} AS SELECT * FROM (VALUES
    ${valuesSql}
  ) AS t(${columnList})`,
  );
}

async function dropRelations(name: string) {
  try {
    await dfCtx.execute_sql(`DROP VIEW IF EXISTS ${name}`);
  } catch (err) {
    console.warn(`Failed to drop view ${name}:`, err);
  }
  try {
    await dfCtx.execute_sql(`DROP TABLE IF EXISTS ${name}`);
  } catch (err) {
    console.warn(`Failed to drop table ${name}:`, err);
  }
}

function sqlLiteral(value: string | number | null): string {
  if (value === null) {
    return "NULL";
  }
  if (typeof value === "number") {
    return value.toString();
  }
  const escaped = value.replace(/'/g, "''");
  return `'${escaped}'`;
}

function parseFastqSafe(text: string): FastqRecord[] {
  if (!text.trim()) {
    return [];
  }
  return parseFastq(text);
}

function parseFastq(text: string): FastqRecord[] {
  const lines = text.split(/\r?\n/);
  const records: FastqRecord[] = [];
  let idx = 0;

  const nextNonempty = () => {
    while (idx < lines.length) {
      const line = lines[idx++]?.trimEnd();
      if (line && line.length > 0) {
        return line;
      }
    }
    return undefined;
  };

  for (;;) {
    const tag = nextNonempty();
    if (!tag) {
      break;
    }
    const seq = nextNonempty();
    const plus = nextNonempty();
    const qual = nextNonempty();

    if (!seq || !plus || !qual) {
      throw new Error("Unexpected EOF while parsing FASTQ record");
    }
    if (!tag.startsWith("@")) {
      throw new Error(`Invalid FASTQ tag line: ${tag}`);
    }
    if (!plus.startsWith("+")) {
      throw new Error(`Invalid FASTQ '+' separator: ${plus}`);
    }
    if (seq.length !== qual.length) {
      throw new Error(
        `Sequence and quality lengths differ for ${tag}: ${seq.length} vs ${qual.length}`,
      );
    }

    records.push({ tag, seq, qual });
  }

  return records;
}

function formatFastqSummary(content: string): string {
  const records = parseFastqSafe(content);
  if (records.length === 0) {
    return "0 records";
  }
  return records.length === 1 ? "1 record" : `${records.length} records`;
}
