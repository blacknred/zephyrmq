import type { IClientLister } from "@domain/interfaces/client/IClientLister";

export class ListClients {
  constructor(private lister: IClientLister) {}

  async execute() {
    return this.lister.list();
  }
}
