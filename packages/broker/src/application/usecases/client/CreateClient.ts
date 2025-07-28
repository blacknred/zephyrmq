import type { ILogService } from "@app/interfaces/ILogService";
import type { IClientCreator } from "@domain/interfaces/client/IClientCreator";
import type { IClientType } from "@domain/interfaces/client/IClientType";

export class CreateClient {
  constructor(
    private creator: IClientCreator,
    private logService?: ILogService
  ) {}

  async execute(type: IClientType) {
    const client = await this.creator.create(type);
    this.logService?.log("Client is created", {
      id: client.id,
      type,
    });

    if (type === 'producer') {
return this.producerFactory.create(id);
    }
    

    return client;
  }
}
