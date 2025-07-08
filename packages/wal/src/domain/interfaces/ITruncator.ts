export interface ITruncator {
  truncate(upToOffset: number): Promise<void>;
}
