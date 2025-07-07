import type { ILogDriver } from "./ILoggerDriver";

export interface ILogger {
  log(msg: string, extra?: object, level?: keyof ILogDriver): void;
  flush: () => void;
  destroy(): void;
}
