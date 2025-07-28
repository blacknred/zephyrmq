import type { ITopic } from "@app/interfaces/ITopic";
import type { ITopicGetter } from "@domain/interfaces/topic/ITopicGetter";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreTopicGetter implements ITopicGetter {
  constructor(private topics: IMap<string, ITopic<any>>) {}

  async get<Data>(name: string): Promise<ITopic<Data> | undefined> {
    return this.topics.get(name);
  }
}
