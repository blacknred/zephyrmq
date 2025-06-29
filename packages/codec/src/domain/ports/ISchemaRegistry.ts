import type { ISchema } from "./ISchema";

export interface ISchemaRegistry {
  addSchema<T>(name: string, schema: ISchema<T>): void;
  removeSchema(name: string): void;
  getSchema<T>(name: string): ISchema<T> | undefined;
}
