import type { IAppender } from "../../domain/interfaces/IAppender";

// name, config
export class GetTopic {
  constructor(private appender: IAppender) {}

  async execute(data: Buffer) {
    return this.appender.append(data);
  }
}
