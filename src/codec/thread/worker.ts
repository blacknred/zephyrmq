import {
  parentPort,
  workerData,
  type TransferListItem,
} from "node:worker_threads";
import { BinaryCodec } from "../core/binary_codec";
import { EncryptedBinaryCodec } from "../core/encrypted_binary_codec";
import { BinarySchemaRegistry } from "../schema/registry";
import { BinarySchema } from "../schema/schema";
import type { WorkerRequest } from "./pool";

const schemaRegistry = new BinarySchemaRegistry();

const codec = workerData
  ? new EncryptedBinaryCodec(workerData)
  : new BinaryCodec();

parentPort?.on("message", (msg: WorkerRequest) => {
  const { id, method, args } = msg;

  try {
    let result: any;

    switch (method) {
      case "encode":
        {
          const [data, schemaRef, compress] = args;
          const buffer = codec.encode(
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
          result = codec.decode(buffer, schema);
        }
        break;
      case "registerSchema":
        {
          const [name, schemaDef] = args;
          const schema = new BinarySchema(schemaDef);
          schemaRegistry.addSchema(name, schema);

          result = true;
        }
        break;
      case "unregisterSchema":
        {
          const [schemaRef] = args;
          schemaRegistry.removeSchema(schemaRef);

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
