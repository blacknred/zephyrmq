export interface IDeliveryAttemptRecorder {
  recordAttempt(messageId: number): number;
}