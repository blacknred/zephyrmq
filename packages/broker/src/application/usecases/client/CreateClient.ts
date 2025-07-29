import type { IConsumerConfig } from "@app/interfaces/IConsumerConfig";
import type { ILogService } from "@app/interfaces/ILogService";
import type { IClientCreator } from "@domain/interfaces/client/IClientCreator";
import type { IClientType } from "@domain/interfaces/client/IClientType";

export class CreateClient {
  constructor(
    private clientCreator: IClientCreator,
    private logService?: ILogService
  ) {}

  async execute(type: IClientType, config: IConsumerConfig = {}) {
    const { id } = await this.clientCreator.create(type);

    if (type === "consumer") {
      const { groupId, routingKeys, noAck, limit } = config;
      this.messageRouter.addConsumer(id, groupId, routingKeys);
      this.queueManager.addQueue(id);
    }

    this.logService?.log("Client is created", { id, type });

    return id;
  }
}
