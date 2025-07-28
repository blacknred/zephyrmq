import { TopicRegistry } from "@app/services/TopicRegistry";

export class TopicRegistryFactory {
  create() {}
}
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

    

    return new TopicRegistry(topics);
  }
}

// 