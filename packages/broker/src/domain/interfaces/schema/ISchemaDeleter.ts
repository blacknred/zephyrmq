export interface ISchemaDeleter {
  delete(schemaId: string): Promise<void>;
}