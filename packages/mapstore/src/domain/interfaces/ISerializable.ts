export interface ISerializable<T = unknown, R = unknown> {
  serialize(data: T, key?: unknown): R;
  deserialize(data: R, key?: unknown): T;
}
