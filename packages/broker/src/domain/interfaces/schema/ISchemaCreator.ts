import type { ISchemaDefinition } from "./ISchemaDefinition";

export interface ISchemaCreator {
  create(
    name: string,
    schemaDef: ISchemaDefinition,
    author?: string
  ): Promise<string | void>;
}
