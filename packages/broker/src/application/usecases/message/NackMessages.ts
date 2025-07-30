import type { ILogService } from "@app/interfaces/ILogService";
import type { IDedupCreator } from "@domain/interfaces/message/dedup/IDedupCreator";
import type { IMessageReader } from "@domain/interfaces/message/IMessageReader";
import type { AckMessages } from "./AckMessages";
import type { RouteMessage } from "./RouteMessage";
import type { IDeliveryRetryDelayProvider } from "@domain/interfaces/message/delivery/IDeliveryRetryDelayProvider";

export class NackMessages {
  constructor(
    private ackMessages: AckMessages,
    private routeMessage: RouteMessage,
    private messageReader: IMessageReader,
    private dedupCreator: IDedupCreator,
    private deliveryDelayProvider: IDeliveryRetryDelayProvider,
    private logService?: ILogService
  ) {}

  async execute(consumerId: number, messageId?: number, requeue = true) {
    const ackedMessageIds = await this.ackMessages.execute(
      consumerId,
      messageId
    );

    for (const messageId of ackedMessageIds) {
      const meta = await this.messageReader.readMetadata(messageId);
      if (!meta) continue;

      this.logService?.log(
        `Message is nacked to ${requeue ? consumerId : "topic"}.`,
        meta,
        "warn"
      );

      // Consumer nacked with requeue=false (as well as AckMonitor nack call) means he rejected to process the message
      if (!requeue) {
        // mark message as processed by consumer
        await this.dedupCreator.create(consumerId, messageId);
        // reroute it with skipDLQ=true to avoid message ends up in dlq due the 'no_consumers' reason.
        // Consumer as well as other consumers which have processed the message will not get it again.
        // Rerouting helps to redeliver message to another consumer group member.
        await this.routeMessage.execute(meta, true);
        continue;
      }

      // Consumer nacked with requeue=true: he will process message later so we send it to consumer queue after backoff
      if (this.pipeline.process(meta)) continue;
      const delay = await this.deliveryDelayProvider.getDelay(messageId);
      meta.ttd = Date.now() - meta.ts + delay;
      this.delayedMessageManager.enqueue(meta, consumerId);
    }

    return ackedMessageIds.length;
  }
}
