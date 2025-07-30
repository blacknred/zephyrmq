import type { IDLQConsumer } from "@app/interfaces/IDLQConsumer";
import type { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { IDLQEntry } from "@domain/interfaces/dlq/IDLQEntry";

export class DLQConsumer<Data> implements IDLQConsumer<Data> {
  static MAX_LIMIT = 1000;
  private reader: AsyncGenerator<IDLQEntry<Data>, void, unknown>;
  constructor(
    private readonly dlqService: IDLQService<Data>,
    public readonly id: number,
    private readonly limit = 1
  ) {
    this.limit = Math.min(DLQConsumer.MAX_LIMIT, limit);
    // singleton reader allows to read only once, waiting for the newest messages to arrive
    this.reader = this.dlqService.createDlqReader();
  }

  async consume() {
    const messages: IDLQEntry<Data>[] = [];

    for await (const message of this.reader) {
      if (!message) break;
      messages.push(message);
      if (messages.length == this.limit) break;
    }

    return messages;
  }

  async replayDlq(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    return this.dlqService.replayDlq(this.id, handler, filter);
  }
}