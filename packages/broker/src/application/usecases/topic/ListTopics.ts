import type { ITopicLister } from "@domain/interfaces/topic/ITopicLister";

export class ListTopics {
  constructor(private lister: ITopicLister) {}

  async execute() {
    return this.lister.list();
  }
}
