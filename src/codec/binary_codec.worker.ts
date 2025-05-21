import { parentPort, workerData } from "node:worker_threads";
import { BinaryCodec } from "./binary_codec";

// src/codecs/worker/codec-worker.ts
const codec = new BinaryCodec();

parentPort!.on("message", async (msg) => {
  const { id, method, args } = msg;

  try {
    const result = await (codec as any)[method].apply(codec, args);
    parentPort!.postMessage({ id, result });
  } catch (err) {
    parentPort!.postMessage({ id, error: (err as Error).toString() });
  }
});
