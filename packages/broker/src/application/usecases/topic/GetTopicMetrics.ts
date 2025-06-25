import type { IAppender } from "../../domain/interfaces/IAppender";

// name, config, metrics
export class GetTopicMetrics {
  constructor(private appender: IAppender) {}

  async execute(data: Buffer) {
    return this.appender.append(data);
  }
}
