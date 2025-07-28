import type { ILogService } from "@app/interfaces/ILogService";
import type { ITopicConfig } from "@domain/interfaces/topic/ITopicConfig";
import type { ITopicCreator } from "@domain/interfaces/topic/ITopicCreator";

export class CreateTopic {
  constructor(
    private creator: ITopicCreator,
    private logService?: ILogService
  ) {}

  async execute(name: string, config: ITopicConfig) {
    const topic = await this.creator.create(name, config);
    this.logService?.log("Topic is created", {
      ...config,
      name,
    });

    return topic;
  }
}
