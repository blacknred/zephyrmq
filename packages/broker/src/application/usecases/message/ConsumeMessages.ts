import type { ILogService } from "@app/interfaces/ILogService";
import type { IClientActivityRecorder } from "@domain/interfaces/client/IClientActivityRecorder";
import type { IMessageReader } from "@domain/interfaces/message/IMessageReader";
import type { IAckCreator } from "@domain/interfaces/message/ack/IAckCreator";
import type { IMaxUnackedChecker } from "@domain/interfaces/message/ack/IMaxUnackedChecker";
import type { IDedupCreator } from "@domain/interfaces/message/dedup/IDedupCreator";
import type { IDeliveryConfirmer } from "@domain/interfaces/message/delivery/IDeliveryConfirmer";
import type { IQueueDequeuer } from "@domain/interfaces/message/queue/IQueueDequeuer";

export class ConsumeMessages {
  constructor(
    private maxUnackedChecker: IMaxUnackedChecker,
    private ackCreator: IAckCreator,
    private activityRecorder: IClientActivityRecorder,
    private dedupCreator: IDedupCreator,
    private deliveryConfirmer: IDeliveryConfirmer,
    private messageReader: IMessageReader,
    private dequeuer: IQueueDequeuer,
    private logService?: ILogService
  ) {}

  async execute<Data>(consumerId: number, limit: number, noAck = false) {
    const messages: Data[] = [];

    while (messages.length < limit) {
      if (await this.maxUnackedChecker.hasExceeded(consumerId)) break;

      const messageId = await this.dequeuer.dequeue(consumerId);
      if (!messageId) break;

      const [message, meta] = await this.messageReader.read<Data>(messageId);
      if (!meta || !message) break;

      messages.push(message);

      if (!noAck) {
        await this.ackCreator.create(consumerId, messageId);
        await this.activityRecorder.record(consumerId, {
          pendingAcks: 1,
          status: "active",
        });
      } else {
        await this.activityRecorder.record(consumerId, {
          messageCount: 1,
          pendingAcks: 0,
          processingTime: 0,
          status: "idle",
        });

        await this.dedupCreator.create(consumerId, messageId);
        await this.deliveryConfirmer.confirm(messageId);
      }

      this.logService?.log(`Message is consumed.`, meta);
    }

    return messages;
  }
}
