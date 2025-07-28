import type { ITopic } from "./ITopic";

export interface ITopicGetter {
  get<T>(name: string): Promise<ITopic<T> | undefined>;
}
