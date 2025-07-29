import type { ILogService } from "@app/interfaces/ILogService";
import type { IClientDeleter } from "@domain/interfaces/client/IClientDeleter";

export class DeleteClient {
  constructor(
    private clientDeleter: IClientDeleter,
    private logService?: ILogService
  ) {}

  async execute(id: number) {
    this.clientDeleter.delete(id);
    this.messageRouter.removeConsumer(id);
    this.queueManager.removeQueue(id);

    this.logService?.log("Client is deleted", { id });
  }
}
