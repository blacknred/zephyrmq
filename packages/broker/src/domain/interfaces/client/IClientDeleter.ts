export interface IClientDeleter {
  delete(id: number): Promise<void>;
}
