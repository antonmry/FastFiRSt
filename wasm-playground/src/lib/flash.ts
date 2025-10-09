export interface FlashResult {
  combined: string;
  notCombined1: string;
  notCombined2: string;
}

interface FlashExports extends WebAssembly.Exports {
  memory: WebAssembly.Memory;
  flash_alloc(len: number): number;
  flash_dealloc(ptr: number, len: number): void;
  flash_run(forwardPtr: number, forwardLen: number, reversePtr: number, reverseLen: number): number;
  flash_result_len(): number;
  flash_free_result(ptr: number, len: number): void;
}

let flashInstancePromise: Promise<FlashExports> | null = null;

async function loadFlashWasm(): Promise<FlashExports> {
  if (!flashInstancePromise) {
    flashInstancePromise = fetchFlashModule()
      .then((bytes) => WebAssembly.instantiate(bytes, {}))
      .then(({ instance }) => instance.exports as FlashExports);
  }
  return flashInstancePromise;
}

async function fetchFlashModule(): Promise<ArrayBuffer> {
  const base = import.meta.env.BASE_URL ?? "/";
  const url = `${base}flash_wasm.wasm`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`failed to load flash_wasm.wasm: ${response.status} ${response.statusText}`);
  }
  return response.arrayBuffer();
}

function getMemoryView(exports: FlashExports): Uint8Array {
  return new Uint8Array(exports.memory.buffer);
}

export async function runFlash(forward: Uint8Array, reverse: Uint8Array): Promise<FlashResult> {
  const exports = await loadFlashWasm();
  const forwardPtr = exports.flash_alloc(forward.length);
  const reversePtr = exports.flash_alloc(reverse.length);

  // Copy input FASTQ bytes into wasm memory.
  getMemoryView(exports).set(forward, forwardPtr);
  getMemoryView(exports).set(reverse, reversePtr);

  const resultPtr = exports.flash_run(forwardPtr, forward.length, reversePtr, reverse.length);
  const resultLen = exports.flash_result_len();

  const resultBytes = getMemoryView(exports).slice(resultPtr, resultPtr + resultLen);
  exports.flash_free_result(resultPtr, resultLen);
  exports.flash_dealloc(forwardPtr, forward.length);
  exports.flash_dealloc(reversePtr, reverse.length);

  const jsonText = new TextDecoder().decode(resultBytes);
  const payload = JSON.parse(jsonText) as
    | { status: "ok"; combined: string; not_combined_1: string; not_combined_2: string }
    | { status: "error"; error: string };

  if (payload.status === "ok") {
    return {
      combined: payload.combined,
      notCombined1: payload.not_combined_1,
      notCombined2: payload.not_combined_2,
    };
  }

  throw new Error(payload.error ?? "FLASH execution failed");
}
