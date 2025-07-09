import os from "os";
import { join } from "path";
import { WorkerPool } from "../threading/WorkerPool";

export class WorkerPoolFactory {
  create(workerData?: unknown, workersCount?: number) {
    const workerPath = join(__dirname, "../worker/WorkerThread.js");
    const count = workersCount ?? os.cpus().length;

    return new WorkerPool(workerPath, workerData, count);
  }
}
