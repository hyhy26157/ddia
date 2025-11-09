from dataclasses import dataclass, field
from datetime import datetime
from typing import List
import random
import time

DATA_VELOCITY_MAX = 2.0
DATA_VELOCITY_MIN = 0.5

@dataclass
class DataSchema:
    device_id: int = field(default_factory=lambda: random.choice([1, 2, 3]))
    timestamp: int = field(default_factory=lambda: int(datetime.today().timestamp()))
    temperature: float = field(default_factory=lambda: random.uniform(15.0, 30.0))

    
def data_generator():
    data_velocity = random.uniform(DATA_VELOCITY_MIN,DATA_VELOCITY_MAX)
    data = DataSchema()

    time.sleep(data_velocity)
    return data