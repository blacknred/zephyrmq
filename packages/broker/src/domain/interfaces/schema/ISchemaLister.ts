export interface ISchemaLister {
  list(): Promise<string[]>;
}
