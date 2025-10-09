import { Alert, Button, FileInput, Group, Loader, Stack, Text, Textarea } from "@mantine/core";
import { useState } from "react";
import { runFlash, type FlashResult } from "../lib/flash";

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

      const flashResult = await runFlash(new Uint8Array(forwardBuffer), new Uint8Array(reverseBuffer));
      setResult(flashResult);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setResult(null);
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
        the Rust/WebAssembly port of flash-lib.
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
        <Alert color="red" icon={<div className="i-tabler-alert-triangle" style={{ width: 16, height: 16 }} />}>
          {error}
        </Alert>
      )}

      {result && (
        <Stack gap="sm">
          <Text fw={600}>Outputs</Text>

          {downloadTargets.map((target) => (
            <Stack key={target.label} gap="xs">
              <Group justify="space-between" align="center">
                <Text>{target.label}</Text>
                <Button
                  variant="light"
                  leftSection={<div className="i-tabler-download" style={{ width: 16, height: 16 }} />}
                  onClick={() => handleDownload(target)}
                >
                  Download
                </Button>
              </Group>
              <Textarea
                autosize
                minRows={4}
                maxRows={12}
                value={target.accessor(result)}
                readOnly
              />
            </Stack>
          ))}
        </Stack>
      )}
    </Stack>
  );
}
