import type { ILogService } from "@app/interfaces/ILogService";
import type { ITopic } from "@domain/interfaces/topic/ITopic";
import type { ITopicConfig } from "@domain/interfaces/topic/ITopicConfig";
import type { ITopicRegistry } from "@app/interfaces/ITopicRegistry";
import type { ITopicFactory } from "@infra/factories/ITopicFactory";
import type { IMap, IMapStore, ISerializable } from "@zephyrmq/mapstore";

class TopicSerializer implements ISerializable {
  constructor(private topicFactory: ITopicFactory) {}

  serialize({ name, config }: ITopic<any>) {
    return { name, config };
  }
  deserialize(data: { name: string; config: ITopicConfig }) {
    return this.topicFactory.create(data.name, data.config);
  }
}

class TopicRegistryFactory {
  constructor() {
    // db
    // logger
  }
  create() {
    const serializer = new TopicSerializer(topicFactory);

    return TopicRegistry(topics);
  }
}

export class TopicRegistry implements ITopicRegistry {
  private topics: IMap<string, ITopic<any>>;
  constructor(
    private mapStore: IMapStore,
    private topicFactory: ITopicFactory,
    private logService?: ILogService
  ) {
    this.topics = this.mapStore.createMap<string, ITopic<any>>("topics", {
      serializer: new TopicSerializer(topicFactory),
    });
  }

  create<Data>(name: string, config: ITopicConfig): ITopic<Data> {
    if (this.topics.has(name)) {
      throw new Error("Topic already exists");
    }

    const topic = this.topicFactory.create<Data>(name, config);
    this.topics.set(name, topic);

    this.logService?.log("Topic created", {
      ...config,
      name,
    });

    return topic;
  }

  async get<T>(name: string): Promise<ITopic<T> | undefined> {
    return this.topics.get(name);
  }

  async list() {
    const names: string[] = [];
    for await (const name of this.topics.keys()) {
      names.push(name);
    }

    return names;
  }

  delete(name: string): void {
    if (!this.topics.has(name)) {
      throw new Error("Topic not found");
    }

    this.topics.delete(name);
    this.logService?.log("Topic deleted", { name });
  }
}
