import type { ITopicGetter } from "@domain/interfaces/topic/ITopicGetter";

// name, config
export class GetTopic {
  constructor(private getter: ITopicGetter) {}

  async execute(name: string) {
    return this.getter.get(name);
  }
}
