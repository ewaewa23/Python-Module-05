from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union

sensor_batch = {
    "SENSOR_001": {
        "Type": "Environnemental Data",
        "sensor_batch": ["temp:22.5", "humidity:65", "pressure:1013"]},
    "TRANS_001": {
        "Type": "Financial Data",
        "sensor_batch": ["buy:100", "sell:150", "75"]},
    "EVENT_001": {
        "Type": "System Events",
        "sensor_batch": ["login", "error", "logout"]}
}


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        raise NotImplementedError("method not implemented in his child class")

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        raise NotImplementedError("method not implemented in his child class")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        raise NotImplementedError("method not implemented in his child class")


class StreamProcessor():

    def process_all(self, processor: DataStream, data: List[Any]) -> str:
        return processor.process_batch(data)


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        print("Initializing Sensor Stream...")
        try:
            type = str(data_batch[0])
            list_sensor = list(data_batch[1])
            data_process = [type, list_sensor]
        except ValueError:
            return "Error: data have to be list and string"
        else:
            self.filter_data(data_process)
        # filter_data
        # avg_temp = min(list_sensor).split(':')
        # print(f"Stream ID: {self.stream_id}, Type: {type}")
        # print(f"Processing sensor batch: {list_sensor}")
        # print(f"Sensor analysis: {len(list_sensor)} operations,"
        #       f" avg temp: {avg_temp[1]}°C")
        return data_batch[0]

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        list_sensor = [value.split(':') for value in data_batch[1]]
        print(list_sensor)
        return list_sensor


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        pass


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        pass


def main() -> None:
    print("=== Polymorphic Stream Processing ===\n")

    stream_process = StreamProcessor()

    sensor_stream = SensorStream("SENSOR_001")
    transaction_stream = TransactionStream("TRANS_001")
    event_stream = EventStream("EVENT_001")

    sensor_001 = [x for x in sensor_batch["SENSOR_001"].values()]
    trans_001 = [x for x in sensor_batch["TRANS_001"].values()]
    event_001 = [x for x in sensor_batch["EVENT_001"].values()]
    list_sensor_batch = [sensor_001, trans_001, event_001]
    list_stream = [sensor_stream, transaction_stream, event_stream]

    for i in range(len(list_sensor_batch)):
        stream_process.process_all(list_stream[i], list_sensor_batch[i])


if __name__ == "__main__":
    main()
