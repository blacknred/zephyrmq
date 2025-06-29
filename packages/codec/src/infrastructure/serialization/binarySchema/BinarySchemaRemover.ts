import type { ISchemaRegistry } from "@domain/ports/ISchemaRegistry";
import type { ISchemaRemover } from "@domain/ports/ISchemaRemover";

export class BinarySchemaRemover implements ISchemaRemover {
  constructor(private schemaRegistry: ISchemaRegistry) {}

  remove(name: string) {
    this.schemaRegistry.removeSchema(name);
  }
}
