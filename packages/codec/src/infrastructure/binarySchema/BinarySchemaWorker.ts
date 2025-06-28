import {
  parentPort,
  workerData,
  type TransferListItem,
} from "node:worker_threads";
import type { WorkerRequest } from "../../application/WorkerPool";
import { BinarySchemaDecoder } from "./BinarySchemaDecoder";
import { BinarySchemaEncoder } from "./BinarySchemaEncoder";
import { BinarySchemaRegistrar } from "./BinarySchemaRegistrar";
import { BinarySchemaRegistry } from "./BinarySchemaRegistry";
import { BinarySchemaRemover } from "./BinarySchemaRemover";

const encoder = new BinarySchemaEncoder(workerData?.compressionSizeThreshold);
const decoder = new BinarySchemaDecoder();
const schemaRegistry = new BinarySchemaRegistry(workerData?.precompiled);
const schemaRegistrar = new BinarySchemaRegistrar(schemaRegistry);
const schemaRemover = new BinarySchemaRemover(schemaRegistry);

parentPort?.on("message", (msg: WorkerRequest) => {
  const { id, method, args } = msg;

  try {
    let result: any;

    switch (method) {
      case "encode":
        {
          const [data, schemaRef, compress] = args;
          const buffer = encoder.encode(
            data,
            schemaRegistry.getSchema(schemaRef),
            compress
          );

          // Extract ArrayBuffer for transfer
          result = buffer.buffer;
        }
        break;
      case "decode":
        {
          const [data, schemaRef] = args;
          const { byteOffset, byteLength } = data;
          const arrayBuffer = data.arrayBuffer as ArrayBuffer;

          // Reconstruct Buffer from transferred ArrayBuffer
          const buffer = Buffer.from(arrayBuffer, byteOffset, byteLength);
          const schema = schemaRegistry.getSchema(schemaRef);
          result = decoder.decode(buffer, schema);
        }
        break;
      case "registerSchema":
        {
          const [name, schemaDef] = args;
          schemaRegistrar.register(name, schemaDef);

          result = true;
        }
        break;
      case "removeSchema":
        {
          const [schemaRef] = args;
          schemaRemover.remove(schemaRef);

          result = true;
        }
        break;
      default:
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
