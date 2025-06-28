import type { ISchema } from "./ISchema";

export interface ISizeCalculator {
  calculate<T>(data: T, schema: ISchema<T>): number;
}
