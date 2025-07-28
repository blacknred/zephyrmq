import type { IClientState } from "./IClientState";

export interface IClientGetter {
  get(id: number): Promise<IClientState | undefined>;
}
