import type { ISchemaRegistrar } from "src/domain/interfaces/ISchemaRegistrar";
import type { WorkerPool } from "../WorkerPool";

export class RegisterSchema<T> {
  constructor(
    private schemaRegistrar: ISchemaRegistrar<T>,
    private workerPool: WorkerPool
  ) {}

  async execute(name: string, schema: T) {
    this.schemaRegistrar.register(name, schema);
    return this.workerPool.sendToAll<boolean>("registerSchema", [name, schema]);
  }
}
