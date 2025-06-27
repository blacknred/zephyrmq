import type { ILogger } from "../interfaces/ILogger";

export interface ILogCollector {
  log(msg: string, extra?: object, level?: keyof ILogger): void;
  flush: () => void;
  destroy(): void;
}