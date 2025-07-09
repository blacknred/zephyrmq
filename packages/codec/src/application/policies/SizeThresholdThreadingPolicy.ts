import type { ISchemaBasedSizeCalculator } from "@domain/interfaces/ISchemaBasedSizeCalculator";
import type { ISchemaRegistry } from "@domain/interfaces/ISchemaRegistry";
import type { IThreadingPolicy } from "@domain/interfaces/IThreadingPolicy";

export class SizeThresholdThreadingPolicy implements IThreadingPolicy {
  constructor(
    private sizeCalculator: ISchemaBasedSizeCalculator,
    private schemaRegistry: ISchemaRegistry,
    private threshold: number
  ) {}

  shouldEncodeInThread<T>(data: T, schemaRef?: string): boolean {
    if (!schemaRef) return false;

    const schema = this.schemaRegistry.getSchema<T>(schemaRef);
    if (!schema) return false;

    const size = this.sizeCalculator.calculate(data, schema);
    return size >= this.threshold;
  }

  shouldDecodeInThread(buffer: Buffer): boolean {
    return buffer.length >= this.threshold;
  }
}
