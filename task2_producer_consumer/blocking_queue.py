import collections
import threading
import time
from typing import Any, Optional
 
 
class ThreadSafeBoundedQueue:
    """
    A thread-safe FIFO queue with a hard capacity limit.
 
    Args:
        maxsize (int): Maximum number of items the buffer can hold. Must be ≥ 1.
 
    Typical usage:
        q = ThreadSafeBoundedQueue(maxsize=100)
 
        # Producer thread
        q.put(packet)          # blocks if full
 
        # Consumer thread
        item = q.get()         # blocks if empty
    """
 
    def __init__(self, maxsize: int) -> None:
        if maxsize < 1:
            raise ValueError(f"maxsize must be ≥ 1, got {maxsize!r}")
 
        self._maxsize: int = maxsize
 
        self._deque: collections.deque = collections.deque()
 
        self._mutex = threading.Lock()
 
        self._not_full: threading.Condition = threading.Condition(self._mutex)
 
        self._not_empty: threading.Condition = threading.Condition(self._mutex)
 
        self._stats_lock = threading.Lock()
        self._producer_blocks: int = 0  
        self._consumer_blocks: int = 0   
        self._total_enqueued: int  = 0
        self._total_dequeued: int  = 0
 
    def put(self, item: Any, timeout: Optional[float] = None) -> bool:
        """
        Insert *item* at the tail of the queue.
 
        If the queue is full, the calling thread blocks until:
          (a) A consumer removes an item and notifies _not_full, OR
          (b) The optional *timeout* expires (returns False).
 
        Args:
            item:    Any Python object to enqueue.
            timeout: Maximum seconds to wait for space. None = wait forever.
 
        Returns:
            True  if the item was successfully enqueued.
            False if timed out before space became available.
 
        Raises:
            Nothing — timeout is signalled via the bool return value.
 
        Blocking mechanism:
            with self._not_full:          # acquires _mutex
                while full:
                    self._not_full.wait() # releases _mutex, sleeps
                                          # re-acquires _mutex on wake-up
                self._deque.append(item)
                self._not_empty.notify() # wake ONE sleeping consumer
                                          # _mutex released by `with` exit
        """
        blocked    = False
        deadline   = (time.monotonic() + timeout) if timeout is not None else None
 
        with self._not_full:                         
            while len(self._deque) >= self._maxsize:
                blocked = True
 
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0.0:
                        return False                 
                    self._not_full.wait(timeout=remaining)
                else:
                    self._not_full.wait()
 
            self._deque.append(item)
 
            self._not_empty.notify()

        with self._stats_lock:
            self._total_enqueued += 1
            if blocked:
                self._producer_blocks += 1
 
        return True
 
    def get(self, timeout: Optional[float] = None) -> Any:
        """
        Remove and return the item at the head of the queue.
 
        If the queue is empty, the calling thread blocks until:
          (a) A producer inserts an item and notifies _not_empty, OR
          (b) The optional *timeout* expires (raises TimeoutError).
 
        Args:
            timeout: Maximum seconds to wait for an item. None = wait forever.
 
        Returns:
            The dequeued item.
 
        Raises:
            TimeoutError: If *timeout* elapses before an item is available.
 
        Blocking mechanism:
            with self._not_empty:         # acquires _mutex
                while empty:
                    self._not_empty.wait()# releases _mutex, sleeps
                item = self._deque.popleft()
                self._not_full.notify()   # wake ONE sleeping producer
        """
        blocked  = False
        deadline = (time.monotonic() + timeout) if timeout is not None else None
 
        with self._not_empty:                     
            while len(self._deque) == 0:
                blocked = True
 
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0.0:
                        raise TimeoutError("ThreadSafeBoundedQueue.get() timed out")
                    self._not_empty.wait(timeout=remaining)
                else:
                    self._not_empty.wait()
 
            item = self._deque.popleft()
 
            self._not_full.notify()
 
        with self._stats_lock:
            self._total_dequeued += 1
            if blocked:
                self._consumer_blocks += 1
 
        return item
 
 
    def size(self) -> int:
        """Snapshot of the current number of items (may be stale immediately)."""
        with self._mutex:
            return len(self._deque)
 
    def is_empty(self) -> bool:
        with self._mutex:
            return len(self._deque) == 0
 
    def is_full(self) -> bool:
        with self._mutex:
            return len(self._deque) >= self._maxsize
 
    @property
    def maxsize(self) -> int:
        return self._maxsize
 
 
    def get_stats(self) -> dict:
        """Return a snapshot of internal counters (thread-safe read)."""
        with self._stats_lock:
            return {
                "maxsize":         self._maxsize,
                "current_size":    self.size(),
                "total_enqueued":  self._total_enqueued,
                "total_dequeued":  self._total_dequeued,
                "producer_blocks": self._producer_blocks,
                "consumer_blocks": self._consumer_blocks,
            }
 
    def reset_stats(self) -> None:
        """Reset all analytics counters (call between experiments)."""
        with self._stats_lock:
            self._producer_blocks = 0
            self._consumer_blocks = 0
            self._total_enqueued  = 0
            self._total_dequeued  = 0
 