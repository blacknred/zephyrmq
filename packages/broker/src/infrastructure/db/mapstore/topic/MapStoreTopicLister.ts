import type { ITopic } from "@app/interfaces/ITopic";
import type { ITopicLister } from "@domain/interfaces/topic/ITopicLister";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreTopicLister implements ITopicLister {
  constructor(private topics: IMap<string, ITopic<any>>) {}

  async list(): Promise<string[]> {
    const names: string[] = [];
    for await (const name of this.topics.keys()) {
      names.push(name);
    }

    return names;
  }
}
