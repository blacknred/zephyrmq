import type { ITopicRegistry } from "@app/interfaces/ITopicRegistry";
import type { CreateTopic } from "@app/usecases/topic/CreateTopic";
import type { DeleteTopic } from "@app/usecases/topic/DeleteTopic";
import type { GetTopic } from "@app/usecases/topic/GetTopic";
import type { ListTopics } from "@app/usecases/topic/ListTopics";
import type { UpdateTopic } from "@app/usecases/topic/UpdateTopic";
import type { ITopicConfig } from "@domain/interfaces/topic/ITopicConfig";

export class TopicRegistry implements ITopicRegistry {
  constructor(
    private createTopic: CreateTopic,
    private deleteTopic: DeleteTopic,
    private updateTopic: UpdateTopic,
    private listTopics: ListTopics,
    private getTopic: GetTopic
  ) {}

  async create(name: string, config: ITopicConfig) {
    return this.createTopic.execute(name, config);
  }

  async get(name: string) {
    return this.getTopic.execute(name);
  }

  async list() {
    return this.listTopics.execute();
  }

  async update(name: string, config: Partial<ITopicConfig>) {
    return this.updateTopic.execute(name, config);
  }

  async delete(name: string) {
    this.deleteTopic.execute(name);
  }
}
