import type { ISchemaRegistrar } from "@domain/interfaces/ISchemaRegistrar";
import type { ISchemaRegistry } from "@domain/interfaces/ISchemaRegistry";
import { BinarySchema, type ISchemaDefinition } from "./BinarySchema";

export class BinarySchemaRegistrar
  implements ISchemaRegistrar<ISchemaDefinition<any>>
{
  constructor(private schemaRegistry: ISchemaRegistry) {}

  register<T>(name: string, schema: ISchemaDefinition<T>) {
    this.schemaRegistry.addSchema(name, new BinarySchema(schema));
  }
}
