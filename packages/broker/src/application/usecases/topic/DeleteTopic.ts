import type { ILogService } from "@app/interfaces/ILogService";
import type { ITopicDeleter } from "@domain/interfaces/topic/ITopicDeleter";

export class DeleteTopic {
  constructor(
    private deleter: ITopicDeleter,
    private logService?: ILogService
  ) {}

  async execute(name: string) {
    this.deleter.delete(name);
    this.logService?.log("Topic is deleted", { name });
  }
}
