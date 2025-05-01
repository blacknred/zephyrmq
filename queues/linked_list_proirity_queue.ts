class LinkedNode<Data> {
  public next?: LinkedNode<Data>;
  /**
   * Creates a new LinkedNode.
   * @param data - The data associated with this node.
   * @param priority - The priority of this node.
   */
  constructor(public data: Data, public priority: number) {}
}

export class LinkedListPriorityQueue<Data = any> {
  private count = 0;
  private head?: LinkedNode<Data>;

  enqueue(data: Data, priority = 0) {
    const node = new LinkedNode<Data>(data, priority);
    // if empty or new item has highest priority
    if (!this.head || priority > this.head.priority) {
      node.next = this.head;
      this.head = node;
    } else {
      let current = this.head;
      // find insertion point
      while (current.next && priority <= current.next.priority) {
        current = current.next;
      }
      node.next = current.next;
      current.next = node;
    }
    this.count++;
  }

  dequeue() {
    if (!this.head) return;
    const { data } = this.head;
    this.head = this.head.next;
    this.count--;
    return data;
  }

  peek() {
    return this.head?.data;
  }

  isEmpty() {
    return this.count === 0;
  }

  size() {
    return this.count;
  }

  clear() {
    this.head = undefined;
    this.count = 0;
  }
}
