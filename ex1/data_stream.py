from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union

sensor_batch = {
    "SENSOR_001": {
        "Type": "Environnemental Data",
        "sensor_batch": {"temp": 22.5, "humidity": 65, "pressure": 1013}},
    "TRANS_001": {
        "Type": "Financial Data",
        "sensor_batch": [100, 150, 75]},
    "EVENT_001": {
        "Type": "System Events",
        "sensor_batch": ["login", "error", "logout"]}
}
# trans: "buy:60,sell:"

class DataStream(ABC):

    # def __init__(self, stream_id) -> None:
    #     self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class StreamProcessor():
    pass


class SensorStream(DataStream):
    pass


class TransactionStream(DataStream):
    pass


class EventStream(DataStream):
    pass


def main() -> None:
    print("=== Polymorphic Stream Processing ===")
    pass


if __name__ == "__main__":
    main()
