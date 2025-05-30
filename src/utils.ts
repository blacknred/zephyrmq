export const wait = (ms = 1000, signal?: AbortSignal) =>
  new Promise((r, j) => {
    signal?.addEventListener("abort", j);
    setTimeout(r, ms);
  });

export const uniqueIntGenerator = () => {
  return Math.floor(Date.now() * Math.random());
};
