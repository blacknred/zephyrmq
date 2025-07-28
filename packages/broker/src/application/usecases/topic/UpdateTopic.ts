import type { ILogService } from "@app/interfaces/ILogService";
import type { ITopicConfig } from "@domain/interfaces/topic/ITopicConfig";
import type { ITopicUpdater } from "@domain/interfaces/topic/ITopicUpdater";

// config
export class UpdateTopic {
  constructor(
    private updater: ITopicUpdater,
    private logService?: ILogService
  ) {}

  async execute(name: string, config: Partial<ITopicConfig>) {
    const topic = await this.updater.update(name, config);
    this.logService?.log("Topic is updated", { name });

    return topic;
  }
}
