import type { ISchema } from "./ISchema";

export interface ISchemaBasedSizeCalculator {
  calculate<T>(data: T, schema: ISchema<T>): number;
}
