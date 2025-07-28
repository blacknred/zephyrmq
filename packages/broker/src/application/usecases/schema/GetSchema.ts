import type { ISchemaGetter } from "@domain/interfaces/schema/ISchemaGetter";

export class GetSchema {
  constructor(private getter: ISchemaGetter) {}

  async execute(schemaId: string) {
    return this.getter.get(schemaId);
  }
}
