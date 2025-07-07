import type { ILogService } from "@app/interfaces/ILogService";
import type { ILogger } from "@domain/ports/ILogger";
import type { ILoggerFactory } from "@domain/ports/ILoggerFactory";

export class LogService implements ILogService {
  private loggers = new Map<string, ILogger>();
  private globalLogger: ILogger;

  constructor(private loggerFactory: ILoggerFactory) {
    this.globalLogger = this.loggerFactory.create();
  }

  log() {
    return this.globalLogger.log;
  }

  for(name: string): ILogger {
    if (!this.loggers.has(name)) {
      this.loggers.set(name, this.loggerFactory.create(name));
    }
    return this.loggers.get(name)!;
  }

  flushAll(): void {
    this.globalLogger.flush();
    this.loggers.forEach((logger) => logger.flush());
  }
}
