import type { ICodec } from "@app/interfaces/ICodec";
import type { WorkerRequest } from "@domain/interfaces/IWorkerPool";
import type { TransferListItem } from "node:worker_threads";

export class WorkerMessageHandler {
  constructor(private codec: ICodec) {}

  async handle(
    msg: WorkerRequest
  ): Promise<{ result: unknown; transferList?: TransferListItem[] }> {
    const { method, args } = msg;

    switch (method) {
      case "encode": {
        const [data, schemaRef, compress] = args;
        const buffer = await this.codec.encode(data, schemaRef, compress);
        return { result: buffer.buffer, transferList: [buffer.buffer] };
      }
      case "decode": {
        const [data, schemaRef] = args;
        const { byteOffset, byteLength } = data;
        const arrayBuffer = data.arrayBuffer as ArrayBuffer;
        const buffer = Buffer.from(arrayBuffer, byteOffset, byteLength);
        const result = await this.codec.decode(buffer, schemaRef);
        return { result };
      }
      case "registerSchema": {
        const [name, schema] = args;
        await this.codec.registerSchema(name, schema);
        return { result: undefined };
      }
      case "removeSchema": {
        const [schemaRef] = args;
        await this.codec.removeSchema(schemaRef);
        return { result: undefined };
      }
      default:
        throw new Error(`Unknown method: ${method}`);
    }
  }
}
