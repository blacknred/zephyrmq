import type { ISchema } from "src/domain/interfaces/ISchema";
import type { ISchemaRegistry } from "src/domain/interfaces/ISchemaRegistry";

export class BinarySchemaRegistry implements ISchemaRegistry {
  private schemas: Map<any, ISchema<any>>;

  constructor(precompiled?: Record<string, ISchema<unknown>>) {
    this.schemas = new Map<any, ISchema<any>>(
      Object.entries(precompiled ?? {})
    );
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
