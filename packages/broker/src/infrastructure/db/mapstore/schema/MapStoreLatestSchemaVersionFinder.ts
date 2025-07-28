import type { ILatestSchemaVersionFinder } from "@domain/interfaces/schema/ILatestSchemaVersionFinder";
import type { ISchema } from "@domain/interfaces/schema/ISchema";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreLatestSchemaVersionFinder
  implements ILatestSchemaVersionFinder
{
  constructor(private schemas: IMap<string, ISchema<any>>) {}

  async findLatestVersion(name: string): Promise<string | undefined> {
    const candidates = [];

    for await (const key of this.schemas.keys()) {
      if (key.startsWith(name + ":")) {
        candidates.push({ key, version: parseInt(key.split(":")[1]) });
      }
    }

    candidates.sort((a, b) => b.version - a.version);

    return candidates[0]?.key;
  }
}
