import type { ISchemaRemover } from "@domain/ports/ISchemaRemover";
import type { WorkerPool } from "@infra/worker/WorkerPool";

export class RemoveSchema {
  constructor(
    private schemaRemover: ISchemaRemover,
    private workerPool: WorkerPool
  ) {}

  async execute(name: string) {
    this.schemaRemover.remove(name);
    return this.workerPool.sendToAll<boolean>("removeSchema", [name]);
  }
}
