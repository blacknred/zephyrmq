import type { IConsumerConfig } from "@app/interfaces/IConsumerConfig";
import type { ITopic } from "@app/interfaces/ITopic";
import type { CreateClient } from "@app/usecases/client/CreateClient";
import type { DeleteClient } from "@app/usecases/client/DeleteClient";
import type { GetClient } from "@app/usecases/client/GetClient";
import type { ListClients } from "@app/usecases/client/ListClients";
import type { PublishMessage } from "@app/usecases/message/PublishMessage";
import type { RouteMessage } from "@app/usecases/message/RouteMessage";
import type { IClientType } from "@domain/interfaces/client/IClientType";
import { Producer } from "./Producer";

export class Topic implements ITopic<any> {
  constructor(
    private _createClient: CreateClient,
    private _deleteClient: DeleteClient,
    private _listClients: ListClients,
    private _getClient: GetClient,
    private publishMessage: PublishMessage,
    private routeMessage: RouteMessage,
  ) {}

  async createProducer() {
    const id = await this._createClient.execute('producer');
    
    return new Producer(
      this.publishMessage,
      this.routeMessage,
      id
    );
  }

  async createConsumer(config: IConsumerConfig = {}) {
    const id = await this._createClient.execute('consumer', config);
    
    return new Consumer(
      this.consumptionService,
      this.ackService,
      this.subscriptionService,
      id,
      noAck,
      limit
    );
  }

  async createDLQConsumer(config: IConsumerConfig = {}) {
    const id = await this._createClient.execute('consumer', config);
    return new DLQConsumer(this.dlqService, id, limit);
  }

  async getClient(id: number) {
    return this._getClient.execute(id);
  }

  async listClients() {
    return this._listClients.execute();
  }

  async deleteClient(id: number) {
    return this._deleteClient.execute(id);
  }

  // async dispose() {}

  // async getMetrics() {
  //   return {
  //     name: this.name,
  //     subscriptions: this.subscriptionService.getMetrics(),
  //     clients: this.clientService.getMetrics(),
  //     dlq: this.dlqService.getMetrics(),
  //     storage: await this.storageService.getMetrics(),
  //     ...this.ackService.getMetrics(),
  //     ...this.publishingService.getMetrics(),
  //     ...this.consumptionService.getMetrics(),
  //     ...this.metrics.getMetrics(),
  //   };
  // }
}