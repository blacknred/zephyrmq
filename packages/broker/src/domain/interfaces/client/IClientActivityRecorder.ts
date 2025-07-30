import type { IMutableClientState } from "./IClientState";

export interface IClientActivityRecorder {
  record(id: number, activityRecord: Partial<IMutableClientState>): Promise<void>;
}