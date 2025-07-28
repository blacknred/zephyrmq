import type { ISchema } from "@domain/interfaces/schema/ISchema";
import type { ISchemaLister } from "@domain/interfaces/schema/ISchemaLister";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreSchemaLister implements ISchemaLister {
  constructor(private schemas: IMap<string, ISchema<any>>) {}

  async list(): Promise<string[]> {
    const names = new Set<string>();
    for await (const key of this.schemas.keys()) {
      names.add(key.split(":")[0]);
    }

    return Array.from(names).map((name) => `${name}:latest`);
  }
}
