import type { ISchemaDefinition } from "./ISchemaDefinition";

export interface ISchema<T> {
  schemaDef: ISchemaDefinition<T>;
  author?: string;
  ts: number;
}
