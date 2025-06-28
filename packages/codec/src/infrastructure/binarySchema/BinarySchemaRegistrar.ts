import type { ISchemaRegistrar } from "src/domain/interfaces/ISchemaRegistrar";
import type { ISchemaRegistry } from "src/domain/interfaces/ISchemaRegistry";
import { BinarySchema, type ISchemaDefinition } from "./BinarySchema";

export class BinarySchemaRegistrar
  implements ISchemaRegistrar<ISchemaDefinition<any>>
{
  constructor(private schemaRegistry: ISchemaRegistry) {}

  register<T>(name: string, schema: ISchemaDefinition<T>) {
    this.schemaRegistry.addSchema(name, new BinarySchema(schema));
  }
}
