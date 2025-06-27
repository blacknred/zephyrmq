interface IMessageFactory<Data> {
  create(
    batch: Data[],
    metadataInput: MetadataInput & {
      topic: string;
      producerId: number;
    }
  ): Promise<IMessageCreationResult[]>;
}

export class MessageFactory<Data> implements IMessageFactory<Data> {
  constructor(
    private codec: ICodec,
    private validators: IMessageValidator<Data>[],
    private schemaId?: string
  ) {}

  async create(
    batch: Data[],
    metadataInput: MetadataInput & { topic: string; producerId: number }
  ) {
    return Promise.all(
      batch.map(async (data) => {
        const meta = new MessageMetadata();
        Object.assign(meta, metadataInput);
        meta.id = uniqueIntGenerator();

        try {
          const message = await this.codec.encode(data, this.schemaId);
          this.validators.forEach((v) =>
            v.validate({ data, size: message.length, dedupId: meta.dedupId })
          );
          return { meta, message };
        } catch (error) {
          return { meta, error };
        }
      })
    );
  }
}

// MessageMetadata, ClientState
// ISchemaDefinition, ISchemaValidator, ISchemaSerializer,
// ITopicRegistry
// IClientRegistry
// ISubscriptionRegistry
// IDeadLetterRegistry
// IDeliveryRegistry, IProcessedMessageRegistry, IAckRegistry
// IQueueRegistry
// IDeduplicationRegistry, IConsumerGroup, IConsumerGroupRegistry, IHashRing, IHashRingStore, IDelayedQueue
// 






