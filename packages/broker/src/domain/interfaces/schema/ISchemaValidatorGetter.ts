import type { ISchemaValidator } from "./ISchemaValidator";

export interface ISchemaValidatorGetter {
  get(name: string): Promise<ISchemaValidator | undefined>;
}
