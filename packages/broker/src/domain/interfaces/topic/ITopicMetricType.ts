export type ITopicMetricType =
  | "totalMessagesPublished"
  | "totalBytes"
  | "ts"
  | "depth"
  | "enqueueRate"
  | "dequeueRate"
  | "avgLatencyMs"; // Time in queue
