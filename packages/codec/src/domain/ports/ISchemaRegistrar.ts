export interface ISchemaRegistrar<T> {
  register(name: string, schema: T): void;
}
