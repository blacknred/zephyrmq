import type { IClientGetter } from "@domain/interfaces/client/IClientGetter";

export class GetClient {
  constructor(private getter: IClientGetter) {}

  async execute(id: number) {
    return this.getter.get(id);
  }
}
