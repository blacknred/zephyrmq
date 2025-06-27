import type { ILogCollector } from "./ILogCollector";

export interface ILogService {
  globalCollector: ILogCollector;
  forTopic(name: string): ILogCollector;
  flushAll(): void;
}
