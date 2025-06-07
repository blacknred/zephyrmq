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
// ITopic
// ISchemaDefinition, ISchemaValidator, ISchemaSerializer,
// IClientRegistry, 


// CreateTopicUseCase, UpdateTopicConfigUseCase, ListTopicsUseCase, 
// GetTopicUseCase(withMetrics), DeleteTopicUseCase, CollectTopicMetricsUseCase
// 
// RegisterSchemaDefinitionUseCase, RemoveSchemaDefinitionUseCase
// ListSchemaDefinitionUseCase, GetSchemaDefinition, GetSchemaValidator


// domain: entities and interfaces for infra classes
// infra: work with io/cpu bound side effects
// application: usecases(atomic domain operation, mostly public api) and non-state non-sideeffect 'utility' classes
// factories: dedicated