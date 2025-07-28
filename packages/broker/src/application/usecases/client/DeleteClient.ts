import type { ILogService } from "@app/interfaces/ILogService";
import type { IClientDeleter } from "@domain/interfaces/client/IClientDeleter";

export class DeleteClient {
  constructor(
    private deleter: IClientDeleter,
    private logService?: ILogService
  ) {}

  async execute(id: number) {
    this.deleter.delete(id);
    this.logService?.log("Client is deleted", { id });
  }
}
