import type { TransferListItem } from "node:worker_threads";
import { parentPort } from "node:worker_threads";
import { BinaryCodec } from "./binary_codec";
import type { WorkerRequest } from "./binary_codec.threaded";

const codec = new BinaryCodec();

parentPort!.on("message", async (msg: WorkerRequest) => {
  const { id, method, args } = msg;

  try {
    let result: any;

    if (method === "encode") {
      const [data] = args;
      const buffer = await codec.encode(data);

      // extract ArrayBuffer for transfer
      result = buffer.buffer;
    } else if (method === "decode") {
      const [info] = args;
      const { byteOffset, byteLength } = info;
      const arrayBuffer = info.arrayBuffer as ArrayBuffer;

      // Reconstruct Buffer from transferred ArrayBuffer
      const buffer = Buffer.from(arrayBuffer, byteOffset, byteLength);
      result = await codec.decode(buffer);
    } else if (method === "encodeMetadata") {
      const [meta] = args;
      const buffer = await codec.encodeMetadata(meta);

      // extract ArrayBuffer for transfer
      result = buffer.buffer;
    } else if (method === "decodeMetadata") {
      const [info, keys] = args;
      const { byteOffset, byteLength } = info;
      const arrayBuffer = info.arrayBuffer as ArrayBuffer;

      // Reconstruct Buffer from transferred ArrayBuffer
      const buffer = Buffer.from(arrayBuffer, byteOffset, byteLength);
      result = await codec.decodeMetadata(buffer, keys);
    } else if (method === "updateMetadata") {
      const [info, partialMeta] = args;
      const { byteOffset, byteLength } = info;
      const arrayBuffer = info.arrayBuffer as ArrayBuffer;

      // Reconstruct Buffer from transferred ArrayBuffer
      const buffer = Buffer.from(arrayBuffer, byteOffset, byteLength);
      const updatedBuffer = await codec.updateMetadata(buffer, partialMeta);
      result = updatedBuffer.buffer;
    } else {
      throw new Error(`Unknown method: ${method}`);
    }

    // If result is an ArrayBuffer, transfer it
    if (result instanceof ArrayBuffer) {
      parentPort!.postMessage({ id, result }, [result]);
    } else if (isTransferableObject(result)) {
      const transferables = collectTransferables(result);
      parentPort!.postMessage({ id, result }, transferables);
    } else {
      parentPort!.postMessage({ id, result });
    }
  } catch (err) {
    parentPort!.postMessage({ id, error: (err as Error).message });
  }
});

// Helper: Detect if object contains transferable objects
function isTransferableObject(obj: any): boolean {
  if (!obj || typeof obj !== "object") return false;
  if (obj instanceof ArrayBuffer) return true;
  for (const key in obj) {
    if (isTransferableObject(obj[key])) return true;
  }
  return false;
}

// Helper: Collect all transferable objects
function collectTransferables(obj: any): TransferListItem[] {
  const transferables: TransferListItem[] = [];
  function walk(val: any) {
    if (val instanceof ArrayBuffer) {
      transferables.push(val);
    } else if (val && typeof val === "object") {
      for (const key in val) {
        walk(val[key]);
      }
    }
  }
  
  walk(obj);
  return transferables;
}
