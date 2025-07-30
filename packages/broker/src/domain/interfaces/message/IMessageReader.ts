import type { MessageMetadata } from "@domain/entities/MessageMetadata";

export interface IMessageReader {
  read<Data>(
    id: number
  ): Promise<
    [
      Awaited<Data> | undefined,
      Pick<MessageMetadata, keyof MessageMetadata> | undefined,
    ]
  >;
  readMessage<Data>(id: number): Promise<Data | undefined>;
  readMetadata<K extends keyof MessageMetadata>(
    id: number,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K> | undefined>;
}
