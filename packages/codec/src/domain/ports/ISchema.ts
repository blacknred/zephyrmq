export interface ISchema<T> {
  serialize(data: T): Buffer;
  deserialize(buffer: Buffer): T;
}
