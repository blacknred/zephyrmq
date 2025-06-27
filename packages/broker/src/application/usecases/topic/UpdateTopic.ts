import type { IAppender } from "../../domain/interfaces/IAppender";

// config
export class UpdateTopic {
  constructor(private appender: IAppender) {}

  async execute(data: Buffer) {
    return this.appender.append(data);
  }
}
