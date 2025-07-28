import type { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { IMetadataInput } from "./IProducer";

interface IMessageCreationResult {
  meta: MessageMetadata;
  message?: Buffer;
  error?: any;
}

export interface IMessageFactory<Data> {
  create(
    batch: Data[],
    metadataInput: IMetadataInput & {
      topic: string;
      producerId: number;
    }
  ): Promise<IMessageCreationResult[]>;
}
