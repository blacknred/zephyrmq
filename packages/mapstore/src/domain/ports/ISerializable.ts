export interface ISerializable<T = unknown, R = unknown> {
  serialize(data: T): R;
  deserialize(data: R): T;
}
