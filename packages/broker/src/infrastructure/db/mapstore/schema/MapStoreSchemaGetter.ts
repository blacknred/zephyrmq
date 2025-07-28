import type { ILatestSchemaVersionFinder } from "@domain/interfaces/schema/ILatestSchemaVersionFinder";
import type { ISchema } from "@domain/interfaces/schema/ISchema";
import type { ISchemaGetter } from "@domain/interfaces/schema/ISchemaGetter";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreSchemaGetter implements ISchemaGetter {
  constructor(
    private schemas: IMap<string, ISchema<any>>,
    private latestVersionFinder: ILatestSchemaVersionFinder
  ) {}

  async get(schemaId: string) {
    const [name, versionStr] = schemaId.split(":");
    const version = versionStr ? parseInt(versionStr) : undefined;

    if (version) {
      return this.schemas.get(`${name}:${version}`);
    } else {
      const latestKey = await this.latestVersionFinder.findLatestVersion(name);
      if (!latestKey) return undefined;
      return this.schemas.get(latestKey);
    }
  }
}
