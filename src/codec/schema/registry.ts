import precompiled from "./precompiled";
import type { ISchema } from "./types";

export class BinarySchemaRegistry {
  private schemas: Map<any, ISchema<any>>;

  constructor() {
    this.schemas = new Map<any, ISchema<any>>(Object.entries(precompiled));
  }

  addSchema<T>(name: string, schema: ISchema<T>) {
    this.schemas.set(name, schema);
  }

  removeSchema(name: string) {
    this.schemas.delete(name);
  }

  getSchema<T>(name: string) {
    return this.schemas.get(name) as ISchema<T>;
  }
}
