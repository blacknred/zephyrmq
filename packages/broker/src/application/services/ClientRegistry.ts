import type { IClientRegistry } from "@app/interfaces/IClientRegistry";
import type { CreateClient } from "@app/usecases/client/CreateClient";
import type { DeleteClient } from "@app/usecases/client/DeleteClient";
import type { GetClient } from "@app/usecases/client/GetClient";
import type { ListClients } from "@app/usecases/client/ListClients";
import type { IClientType } from "@domain/interfaces/client/IClientType";

export class ClientRegistry implements IClientRegistry {
  constructor(
    private createClient: CreateClient,
    private deleteClient: DeleteClient,
    private listClients: ListClients,
    private getClient: GetClient
  ) {}

  async create(type: IClientType) {
    return this.createClient.execute(type);
  }

  async get(id: number) {
    return this.getClient.execute(id);
  }

  async list() {
    return this.listClients.execute();
  }

  async delete(id: number) {
    this.deleteClient.execute(id);
  }
}
