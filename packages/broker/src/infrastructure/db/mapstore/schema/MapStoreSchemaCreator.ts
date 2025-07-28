import type { ILatestSchemaVersionFinder } from "@domain/interfaces/schema/ILatestSchemaVersionFinder";
import type { ISchema } from "@domain/interfaces/schema/ISchema";
import type { ISchemaCreator } from "@domain/interfaces/schema/ISchemaCreator";
import type { ISchemaDefinition } from "@domain/interfaces/schema/ISchemaDefinition";
import type { IMap } from "@zephyrmq/mapstore";
import { validateSchema } from "json-schema-compatibility";

export class MapStoreSchemaCreator implements ISchemaCreator {
  constructor(
    private schemas: IMap<string, ISchema<any>>,
    private latestVersionFinder: ILatestSchemaVersionFinder,
    private compatibilityMode: "none" | "forward" | "backward" | "full"
  ) {}

  async create(
    name: string,
    schemaDef: ISchemaDefinition,
    author?: string
  ): Promise<string | void> {
    const latestVersion =
      await this.latestVersionFinder.findLatestVersion(name);

    if (latestVersion && this.compatibilityMode !== "none") {
      const latestSchemaDef = await this.schemas.get(latestVersion);

      if (!validateSchema(latestSchemaDef, schemaDef, this.compatibilityMode)) {
        throw new Error(
          `Schema definition ${name} is not compatible in ${this.compatibilityMode} mode`
        );
      }
    }

    const version = latestVersion
      ? parseInt(latestVersion.split(":")[1]) + 1
      : 1;
    const schemaId = `${name}:${version}`;
    const record = { schemaDef, author, ts: Date.now() };
    this.schemas.set(schemaId, record);
    return schemaId;
  }
}
