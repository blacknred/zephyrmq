export interface ITopicLister {
  list(): Promise<string[]>;
}
