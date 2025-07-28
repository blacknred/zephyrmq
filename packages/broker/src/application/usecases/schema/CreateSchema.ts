import type { ILogService } from "@app/interfaces/ILogService";
import type { ISchemaCreator } from "@domain/interfaces/schema/ISchemaCreator";
import type { ISchemaDefinition } from "@domain/interfaces/schema/ISchemaDefinition";
import type { ISchemaDeleter } from "@domain/interfaces/schema/ISchemaDeleter";
import { BinarySchemaDefinitionCompiler, type ICodec } from "@zephyrmq/codec";

export class CreateSchema {
  constructor(
    private creator: ISchemaCreator,
    private deleter: ISchemaDeleter,
    private codec: ICodec,
    private logService?: ILogService
  ) {}

  async execute(name: string, schemaDef: ISchemaDefinition, author?: string) {
    try {
      const schemaId = await this.creator.create(name, schemaDef, author);
      const schema = BinarySchemaDefinitionCompiler.fromJsonSchema(schemaDef);
      await this.codec.registerSchema(schemaId!, schema);

      this.logService?.log("Schema is created", {
        name,
        author,
      });

      return schemaId;
    } catch (error) {
      await this.deleter.delete(name);
      throw error;
    }
  }
}
