import type { ISchemaRegistry } from "@domain/interfaces/ISchemaRegistry";
import type { ISchemaRemover } from "@domain/interfaces/ISchemaRemover";

export class BinarySchemaRemover implements ISchemaRemover {
  constructor(private schemaRegistry: ISchemaRegistry) {}

  remove(name: string) {
    this.schemaRegistry.removeSchema(name);
  }
}
