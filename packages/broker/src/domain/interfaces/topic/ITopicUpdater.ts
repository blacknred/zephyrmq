import type { ITopic } from "@app/interfaces/ITopic";
import type { ITopicConfig } from "./ITopicConfig";

export interface ITopicUpdater {
  update<T>(
    name: string,
    config: Partial<ITopicConfig>
  ): Promise<ITopic<T> | undefined>;
}
