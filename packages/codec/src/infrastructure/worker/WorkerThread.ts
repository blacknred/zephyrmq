import type { WorkerRequest } from "@domain/interfaces/IWorkerPool";
import { BinarySchemaCodecFactory } from "@infra/factories/BinarySchemaCodecFactory";
import { TransferableCollector } from "@infra/threading/TransferableCollector";
import { TransferableDetector } from "@infra/threading/TransferableDetector";
import { parentPort, workerData } from "node:worker_threads";
import { WorkerMessageHandler } from "./WorkerMessageHandler";

if (!parentPort) {
  throw new Error("This file must be run as a worker thread.");
}

const binarySchemaFactory = new BinarySchemaCodecFactory();
const handler = new WorkerMessageHandler(
  binarySchemaFactory.create(workerData)
);

parentPort.on("message", async (msg: WorkerRequest) => {
  const { id } = msg;
  try {
    const { result, transferList } = await handler.handle(msg);

    if (transferList) {
      parentPort!.postMessage({ id, result }, transferList);
    } else if (TransferableDetector.isTransferable(result)) {
      parentPort!.postMessage(
        { id, result },
        TransferableCollector.collect(result)
      );
    } else {
      parentPort!.postMessage({ id, result });
    }
  } catch (err) {
    parentPort!.postMessage({ id, error: (err as Error).message });
  }
});
