import type { ITopic } from "./ITopic";
import type { ITopicConfig } from "./ITopicConfig";

export interface ITopicCreator {
  create<T>(name: string, config: ITopicConfig): Promise<ITopic<T>>;
}
