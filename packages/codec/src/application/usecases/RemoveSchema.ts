import type { ISchemaRemover } from "src/domain/interfaces/ISchemaRemover";
import type { WorkerPool } from "../WorkerPool";

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
