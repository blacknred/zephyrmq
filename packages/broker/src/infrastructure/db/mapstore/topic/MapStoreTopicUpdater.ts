import type { ITopic } from "@app/interfaces/ITopic";
import type { ITopicConfig } from "@domain/interfaces/topic/ITopicConfig";
import type { ITopicUpdater } from "@domain/interfaces/topic/ITopicUpdater";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreTopicUpdater implements ITopicUpdater {
  constructor(private topics: IMap<string, ITopic<any>>) {}
  async update<Data>(
    name: string,
    config: Partial<ITopicConfig>
  ): Promise<ITopic<Data> | undefined> {
    const topic = await this.topics.get(name);
    if (!topic) {
      throw new Error("Topic not found");
    }

    // TODO: update config
    return topic;
  }
}
