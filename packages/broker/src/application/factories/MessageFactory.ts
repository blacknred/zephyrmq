import type { IMessageFactory } from "@app/interfaces/IMessageFactory";
import type { IMetadataInput } from "@app/interfaces/IProducer";
import { MessageMetadata } from "@domain/entities/MessageMetadata";
import { uniqueIntGenerator } from "@infra/utils";
import type { ICodec } from "@zephyrmq/codec";

export class MessageFactory<Data> implements IMessageFactory<Data> {
  constructor(
    private codec: ICodec,
    private validators: IMessageValidator<Data>[],
    private schemaId?: string
  ) {}

  async create(
    batch: Data[],
    metadataInput: IMetadataInput & { topic: string; producerId: number }
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