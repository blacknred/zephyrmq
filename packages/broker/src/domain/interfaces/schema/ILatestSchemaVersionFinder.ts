export interface ILatestSchemaVersionFinder {
  findLatestVersion(name: string): Promise<string | undefined>;
}
