import type { IClientState, IMutableClientState } from "./IClientState";

export interface IClientUpdater {
  update(
    id: number,
    state: IMutableClientState
  ): Promise<IClientState | undefined>;
}
