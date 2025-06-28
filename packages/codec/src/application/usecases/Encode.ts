import type { IEncoder } from "src/domain/interfaces/IEncoder";
import type { ISchemaRegistry } from "src/domain/interfaces/ISchemaRegistry";
import type { ISizeCalculator } from "src/domain/interfaces/ISizeCalculator";
import type { WorkerPool } from "../WorkerPool";

export class Encode<T> {
  constructor(
    private encoder: IEncoder,
    private schemaRegistry: ISchemaRegistry,
    private sizeCalculator: ISizeCalculator,
    private workerPool: WorkerPool,
    private sizeThreshold: number
  ) {}

  async execute(data: T, schemaRef?: string, compress = false) {
    const schema = schemaRef
      ? this.schemaRegistry.getSchema<T>(schemaRef)
      : undefined;

    if (schema) {
      const size = this.sizeCalculator.calculate(data, schema);
      if (size >= this.sizeThreshold) {
        return this.workerPool.send<Buffer>("encode", [
          data,
          schemaRef,
          compress,
        ]);
      }
    }

    return this.encoder.encode(data, schema, compress);
  }
}
