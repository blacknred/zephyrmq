import type { ITopic } from "@domain/interfaces/topic/ITopic";
import type { ITopicConfig } from "@domain/interfaces/topic/ITopicConfig";

export interface ITopicFactory {
  create<Data>(name: string, config?: Partial<ITopicConfig>): ITopic<Data>;
}
