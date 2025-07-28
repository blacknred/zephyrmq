import type { ILogger } from "@domain/interfaces/ILogger";
import type { ILogDriver } from "@app/interfaces/ILogDriver";
import type { ILoggerFactory } from "@app/interfaces/ILoggerFactory";
import { BufferedLogger } from "../logging/BufferedLogger";

export class BufferLoggerFactory implements ILoggerFactory {
  constructor(
    private driver: ILogDriver,
    private chunkSize = 50
  ) {}
  create(label?: string): ILogger {
    return new BufferedLogger(this.driver, this.chunkSize, label);
  }
}
