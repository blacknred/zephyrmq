import type { ISchemaRemover } from "@domain/interfaces/ISchemaRemover";

export class RemoveSchema {
  constructor(private schemaRemover: ISchemaRemover) {}

  execute(name: string) {
    return this.schemaRemover.remove(name);
  }
}
