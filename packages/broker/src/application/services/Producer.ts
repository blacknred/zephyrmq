import type { IMetadataInput } from "@app/interfaces/IMetadataInput";
import type { IProducer, IPublishResult } from "@app/interfaces/IProducer";
import type { PublishMessage } from "@app/usecases/message/PublishMessage";
import type { RouteMessage } from "@app/usecases/message/RouteMessage";

export class Producer<Data> implements IProducer<Data> {
  constructor(
    private publishMessage: PublishMessage,
    private routeMessage: RouteMessage,
    // private readonly messageFactory: IMessageFactory<Data>,
    // private readonly topicName: string,
    public readonly id: number
  ) {}

  async publish(batch: Data[], metadata: IMetadataInput = {}) {
    const results: IPublishResult[] = [];

    const messages = await this.messageFactory.create(batch, {
      ...metadata,
      topic: this.topicName,
      producerId: this.id,
    });

    for (const { message, meta, error } of messages) {
      const { id, ts } = meta;

      if (error) {
        const err = error instanceof Error ? error.message : "Unknown error";
        results.push({ id, ts, error: err, status: "error" });
        continue;
      }

      try {
        await this.publishMessage.execute(this.id, message!, meta);
        await this.routeMessage.execute(meta);

        results.push({ id, ts, status: "success" });
      } catch (err) {
        const error = err instanceof Error ? err.message : "Unknown error";
        results.push({ id, ts, error, status: "error" });
      }
    }

    return results;
  }
}