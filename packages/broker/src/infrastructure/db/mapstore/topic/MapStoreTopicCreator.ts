import type { ITopic } from "@app/interfaces/ITopic";
import type { ITopicConfig } from "@domain/interfaces/topic/ITopicConfig";
import type { ITopicCreator } from "@domain/interfaces/topic/ITopicCreator";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreTopicCreator implements ITopicCreator {
  constructor(private topics: IMap<string, ITopic<any>>) {}

  async create<Data>(name: string, config: ITopicConfig): Promise<ITopic<Data>> {
    if (this.topics.has(name)) {
      throw new Error("Topic already exists");
    }
    
    const topic = this.topicFactory.create<Data>(name, config);
    this.topics.set(name, topic);

    return topic;
  }
}
