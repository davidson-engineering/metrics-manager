    
from collections import deque
from datetime import datetime

from metrics_metric import Metric

class MetricsBuffer:
    def __init__(self):
        self.buffer = deque()

    def add_metric(self, name, value):
        timestamp = datetime.now()
        metric = Metric(name, value, timestamp) 
        self.buffer.append(metric)

    def clear_buffer(self):
        with self.buffer.mutex:
            self.buffer.clear()

    def get_buffer_copy(self):
        with self.buffer.mutex:
            return list(self.buffer)
        
    def get_buffer_size(self):
        return len(self.buffer)
    
    def not_empty(self):
        return len(self.buffer) > 0
    
    def dump_buffer(self):
        buffer = self.get_buffer_copy()
        self.clear_buffer()
        return buffer