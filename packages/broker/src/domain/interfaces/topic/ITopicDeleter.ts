export interface ITopicDeleter {
  delete(name: string): Promise<void>;
}
