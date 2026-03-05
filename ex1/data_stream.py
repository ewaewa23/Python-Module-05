from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union

sensor_batch = {
    "SENSOR_001": {
        "Type": "Environnemental Data",
        "sensor_batch": ["temp:22.5", "humidity:65", "pressure:1013"]},
    "TRANS_001": {
        "Type": "Financial Data",
        "sensor_batch": ["buy:100", "sell:150", "buy:75"]},
    "EVENT_001": {
        "Type": "System Events",
        "sensor_batch": ["login", "error", "logout"]}
}


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.type = ""

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        raise NotImplementedError("method not implemented in his child class")

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {self.stream_id: self.type}


class StreamProcessor():

    def __init__(self) -> None:
        self.batch_number = 0
        self.data_stream: dict = {}

    def process_all(self, processor: DataStream, data: List[Any],
                    batch_number: int) -> None:
        self.batch_number = 1
        resp = processor.process_batch(data)
        self.data_stream.update({processor.stream_id:
                                 processor.__class__.__name__,
                                 processor.stream_id+"_response": resp})

    def get_info_processed(self) -> None:
        number_sensor = len([value for value in self.data_stream.values()
                             if value == "SensorStream"])
        number_event = len([value for value in self.data_stream.values()
                            if value == "EventStream"])
        number_transaction = len([value for value in self.data_stream.values()
                                  if value == "TransactionStream"])
        sensor_ko = len([value for key, value in self.data_stream.items()
                         if key.split('_')[0] == "SENSOR" and value == "KO"])
        transaction_ko = len([value for key, value in self.data_stream.items()
                              if key.split('_')[0] == "TRANS"
                              and value == "KO"])
        print("=== Polymorphic Stream Processing ===")
        print("Processing mixed stream types through unified interface...\n")
        print(f"Batch {self.batch_number} results")
        print(f"- Sensor data: {number_sensor} readings processed")
        print(f"- Transaction data: {number_transaction} operations"
              " processed")
        print(f"- Event data: {number_event} events processed\n")
        print("Stream filtering active: High-priority data only")
        print(f"Filtered results: {sensor_ko} critical sensor alerts,"
              f" {transaction_ko} large transaction\n")
        print("All streams processed successfully. Nexus throughput optimal.")


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.humidity: int = 0
        self.pressure: int = 0
        self.avg_temp: float = 0
        self.len_batch_sensor: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        print("Initializing Sensor Stream...")
        try:
            self.type = str(data_batch[0])
            list_sensor = list(data_batch[1])
            data_process = [self.type, list_sensor]
            self.len_batch_sensor = len(data_process[1])
            if self.filter_data(data_process)[0] is None:
                raise Exception("value is empty")
        except ValueError:
            print("ERROR: data have to be list and string")
            return "KO"
        except Exception as e:
            print(f"ERROR: {e}")
            return "KO"
        else:
            self.get_stats()
            return "OK"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        try:
            self.avg_temp = float(data_batch[1][0].split(":")[1])
            self.humidity = int(data_batch[1][1].split(":")[1])
            self.pressure = int(data_batch[1][2].split(":")[1])
        except ValueError:
            print("ERROR: value of sensor have to be numeric")
        return data_batch

    def get_stats(self) -> Dict[str, str | int | float]:
        print(f"Stream ID: {self.stream_id}, Type: {self.type}")
        print(f"Processing sensor batch: [{self.avg_temp}, {self.humidity},"
              f" {self.pressure}]")
        print(f"Sensor analysis: {self.len_batch_sensor} readings processed,"
              f" avg temp: {self.avg_temp}°C\n")
        return {'type': self.type, 'humidity': self.humidity,
                'pressure': self.pressure, 'avg_temp': self.avg_temp,
                'len_batch_sensor': self.len_batch_sensor}


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.transaction: list = []
        self.net_flow: int = 0
        self.count_operations: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        print("Initializing Transaction Stream...")
        try:
            self.type = str(data_batch[0])
            self.transaction = list(data_batch[1])
            data_process = [self.type, self.transaction]
            self.count_operations = len(self.transaction)
            if self.filter_data(data_process)[0] is None:
                raise Exception("value is empty")
        except ValueError:
            print("ERROR: data have to be list and string")
            return "KO"
        except Exception as e:
            print(f"ERROR: {e}")
            return "KO"
        else:
            self.get_stats()
            return "OK"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        try:
            for i in range(len(data_batch[1])):
                if data_batch[1][i].split(":")[0] == 'sell':
                    self.net_flow += int(data_batch[1][i].split(":")[1])
                elif data_batch[1][i].split(":")[0] == 'buy':
                    self.net_flow -= int(data_batch[1][i].split(":")[1])
                i += 1
        except ValueError:
            print("ERROR: value in transaction batch have to be numeric")
        return data_batch

    def get_stats(self) -> Dict[str, str | int | float]:
        print(f"Stream ID: {self.stream_id}, Type: {self.type}")
        print(f"Processing transaction batch: {self.transaction}")
        print(f"Transaction analysis: {self.count_operations} operations,"
              f" net flow: {self.net_flow}°C\n")
        return {'type': self.type, 'net_flow': self.net_flow,
                'count_operations': self.count_operations}


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.login: int = 0
        self.error: int = 0
        self.logout: int = 0
        self.count_events: int = 0
        self.event_batch: list = []

    def process_batch(self, data_batch: List[Any]) -> str:
        print("Initializing Event Stream...")
        try:
            self.type = str(data_batch[0])
            self.event_batch = list(data_batch[1])
            data_process = [self.type, self.event_batch]
            self.count_events = len(self.event_batch)
            if self.filter_data(data_process)[0] is None:
                raise Exception("value is empty")
        except ValueError:
            print("ERROR: data have to be list and string")
            return "KO"
        except Exception as e:
            print(f"ERROR: {e}")
            return "KO"
        else:
            self.get_stats()
            return "OK"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        try:
            for i in range(len(data_batch[1])):
                if str(data_batch[1][i]) == 'login':
                    self.login += 1
                elif str(data_batch[1][i]) == 'error':
                    self.error += 1
                elif str(data_batch[1][i]) == 'logout':
                    self.logout += 1
                else:
                    raise ValueError("value have to be login, error, logout")
        except ValueError as e:
            print(f"ERROR: {e}")
        return data_batch

    def get_stats(self) -> Dict[str, str | int | float]:
        print(f"Stream ID: {self.stream_id}, Type: {self.type}")
        print(f"Processing event batch: {self.event_batch}")
        print(f"Event analysis: {self.count_events} events,"
              f" {self.error} error detected\n")
        return {'type': self.type, 'error': self.error,
                'logout': self.logout, 'login': self.login}


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
        stream_process.process_all(list_stream[i], list_sensor_batch[i], 1)

    stream_process.get_info_processed()


if __name__ == "__main__":
    main()
