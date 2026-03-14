"""
Rate limiter usando sliding window — thread-safe.
Garante no máximo max_requests dentro de time_window segundos.
"""
import time
import threading
from collections import deque
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, max_requests: int, time_window: float = 60.0):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: deque = deque()
        self.lock = threading.Lock()

    def wait_if_needed(self):
        """Bloqueia se necessário para respeitar o limite de taxa."""
        with self.lock:
            now = time.time()

            # Remove requisições fora da janela
            while self.requests and self.requests[0] < now - self.time_window:
                self.requests.popleft()

            if len(self.requests) >= self.max_requests:
                sleep_time = self.time_window - (now - self.requests[0])
                if sleep_time > 0:
                    logger.debug(
                        f"Rate limit: aguardando {sleep_time:.2f}s "
                        f"({len(self.requests)}/{self.max_requests})"
                    )
                    time.sleep(sleep_time)
                    now = time.time()
                    while self.requests and self.requests[0] < now - self.time_window:
                        self.requests.popleft()

            self.requests.append(time.time())
