import type { ITopicCreator } from "@domain/interfaces/topic/ITopicCreator";
import type { ITopicDeleter } from "@domain/interfaces/topic/ITopicDeleter";
import type { ITopicGetter } from "@domain/interfaces/topic/ITopicGetter";
import type { ITopicLister } from "@domain/interfaces/topic/ITopicLister";
import type { ITopicUpdater } from "@domain/interfaces/topic/ITopicUpdater";

export interface ITopicRegistry
  extends ITopicCreator,
    ITopicGetter,
    ITopicLister,
    ITopicDeleter,
    ITopicUpdater {}
