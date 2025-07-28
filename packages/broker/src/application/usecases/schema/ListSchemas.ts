import type { ISchemaLister } from "@domain/interfaces/schema/ISchemaLister";

export class ListSchemas {
  constructor(private lister: ISchemaLister) {}

  async execute() {
    return this.lister.list();
  }
}
