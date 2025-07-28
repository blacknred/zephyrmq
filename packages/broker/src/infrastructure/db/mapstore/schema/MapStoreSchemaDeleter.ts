import type { ITopic } from "@app/interfaces/ITopic";
import type { ISchemaDeleter } from "@domain/interfaces/schema/ISchemaDeleter";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreSchemaDeleter implements ISchemaDeleter {
  constructor(private topics: IMap<string, ITopic<any>>) {}

  async delete(name: string): Promise<void> {
    if (!this.topics.has(name)) {
      throw new Error("Topic not found");
    }

    this.topics.delete(name);
  }
}
