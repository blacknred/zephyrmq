import type { MessageMetadata } from "@domain/entities/MessageMetadata";

export interface IMetadataInput
  extends Pick<
    MessageMetadata,
    "priority" | "correlationId" | "ttd" | "ttl" | "dedupId"
  > {}

export interface IPublishResult {
  id: number;
  status: "success" | "error";
  ts: number;
  error?: string;
}

export interface IProducer<Data> {
  id: number;
  publish(batch: Data[], metadata?: IMetadataInput): Promise<IPublishResult[]>;
}
