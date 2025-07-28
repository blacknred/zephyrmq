import type { ISchemaDefRecord } from "@domain/interfaces/schema/ISchema";
import type { ISchemaDefStore } from "@domain/interfaces/schema/ISchemaDefStore";

export class MapStoreSchemaDefStore implements ISchemaDefStore {
  private schemaDefs: PersistedMap<string, ISchemaDefRecord<any>>;

  constructor(mapFactory: IPersistedMapFactory) {
    this.schemaDefs = mapFactory.create("schemaDefs");
  }

  get<T>(schemaId: string): JSONSchemaType<T> | undefined {
    return this.schemaDefs.get(schemaId) as JSONSchemaType<T>;
  }

  set<T>(
    schemaId: string,
    schemaDef: JSONSchemaType<T>,
    author?: string
  ): void {
    const record: ISchemaDefRecord<T> = { schemaDef, author, ts: Date.now() };
    this.schemaDefs.set(schemaId, record);
  }

  delete(schemaId: string): void {
    this.schemaDefs.delete(schemaId);
  }

  list(): string[] {
    return Array.from(this.schemaDefs.keys());
  }
}