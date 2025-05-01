export const wait = (ms = 1000) => new Promise((r) => setTimeout(r, ms));

/**
 * Logs a message to the console with a timestamp and color-coded level.
 *
 * @param message - The message to log.
 * @param level - The severity level of the log message, which determines its color.
 */
export class Logger<
  LEVEL extends "INFO" | "WARN" | "DANGER" = "INFO" | "WARN" | "DANGER"
> {
  colors = { INFO: "green", WARN: "orange", DANGER: "red" } as Record<
    LEVEL,
    string
  >;
  log(message: string, level: LEVEL) {
    const timestamp = new Date().toISOString();
    const color = this.colors[level];
    console.log(`%c ${timestamp} [${level}]`, `color: ${color}`, message);
  }
}
