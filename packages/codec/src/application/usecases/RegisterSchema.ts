import type { ISchemaRegistrar } from "@domain/interfaces/ISchemaRegistrar";

export class RegisterSchema<T> {
  constructor(private schemaRegistrar: ISchemaRegistrar<T>) {}

  execute(name: string, schema: T) {
    return this.schemaRegistrar.register(name, schema);
  }
}
