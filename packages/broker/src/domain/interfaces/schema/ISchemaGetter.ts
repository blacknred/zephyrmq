import type { ISchema } from "./ISchema";

export interface ISchemaGetter {
  get(schemaId: string): Promise<ISchema<any> | undefined>;
}
