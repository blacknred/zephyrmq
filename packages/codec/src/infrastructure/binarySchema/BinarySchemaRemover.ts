import type { ISchemaRegistry } from "src/domain/interfaces/ISchemaRegistry";
import type { ISchemaRemover } from "src/domain/interfaces/ISchemaRemover";

export class BinarySchemaRemover implements ISchemaRemover {
  constructor(private schemaRegistry: ISchemaRegistry) {}

  remove(name: string) {
    this.schemaRegistry.removeSchema(name);
  }
}
