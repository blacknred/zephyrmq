import type { ITopic } from "@app/interfaces/ITopic";
import type { ITopicDeleter } from "@domain/interfaces/topic/ITopicDeleter";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreTopicDeleter implements ITopicDeleter {
  constructor(private topics: IMap<string, ITopic<any>>) {}
  async delete(name: string): Promise<void> {
    if (!this.topics.has(name)) {
      throw new Error("Topic not found");
    }

    this.topics.delete(name);
  }
}
