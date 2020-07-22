from __future__ import annotations
from threading import Lock, Thread
from typing import Dict

# https://refactoring.guru/design-patterns/singleton/python/example#example-1
# https://stackoverflow.com/questions/51896862/how-to-create-singleton-class-with-arguments-in-python
class SingletonMeta(type):
    
    _instances: Dict[Context, Context] = {}
        
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        # https://en.wikipedia.org/wiki/Double-checked_locking
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

# context of the application
class Context(metaclass = SingletonMeta):
 
    def __init__(self, scale: bool = False, gpu: bool = False) -> None:
        self.scale = scale
        self.gpu = gpu


