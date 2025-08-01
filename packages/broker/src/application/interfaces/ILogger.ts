import type { ILogDriver } from "./ILogDriver";

export interface ILogger {
  log(msg: string, extra?: object, level?: keyof ILogDriver): void;
  flush: () => void;
  destroy(): void;
}
