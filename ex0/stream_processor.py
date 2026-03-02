from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        raise NotImplementedError("method not implemented in his child class")

    @abstractmethod
    def validate(self, data: Any) -> bool:
        raise NotImplementedError("method not implemented in his child class")

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        print("Initializing Numeric Processor...")
        try:
            if not data:
                raise ValueError("data is empty")
            processing_data = [value for value in data]
            print(f"Processing data: {processing_data}")
            self.validate(processing_data)
        except ValueError as e:
            return f"ERROR: {e}"
        else:
            return (self.format_output(f"Processed {len(data)} numeric"
                                       f" values, sum={sum(data)},"
                                       f" avg={sum(data)/len(data)}\n"))

    def validate(self, data: Any) -> bool:
        try:
            _ = [int(value) for value in data]
            print("Validation: Numeric data verified")
        except TypeError:
            print("ERROR: value have to be numeric")
            return False
        else:
            return True


class TextProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        print("Initializing Text Processor...")
        try:
            if not data:
                raise ValueError("data is empty")
            processing_data = ' '.join(str(value) for value in data)
            print(f"Processing data: {processing_data}")
            self.validate(processing_data)
        except ValueError as e:
            return f"ERROR: {e}"
        else:
            return (self.format_output(f"Processed text {len(processing_data)}"
                                       f" characters, {len(data)} words\n"))

    def validate(self, data: Any) -> bool:
        try:
            str(data)
            print("Validation: Text data verified")
        except TypeError:
            print("ERROR: value has to be text")
            return False
        else:
            return True


class LogProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        print("Initializing Log Processor...")
        try:
            if not data:
                raise ValueError("data is empty")
            processing_data = str(data)
            print(f"Processing data: {processing_data}")
            self.validate(processing_data)
        except ValueError as e:
            return f"ERROR: {e}"
        else:
            return (self.format_output(processing_data))

    def validate(self, data: Any) -> bool:
        try:
            title_log: str = ""
            i = 0
            while i < 5:
                title_log += data[i]
                i += 1
            if (title_log == "ERROR"):
                print("Validation: Log entry verified")
            else:
                raise ValueError("value has to begin with ERROR")
        except ValueError as e:
            print(f"ERROR: {e}")
            return False
        else:
            return True

    def format_output(self, result: str) -> str:
        return f"Output: [ALERT] {result}\n"


def main():
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    data_num = [1, 2, 3, 4, 5]
    data_str = ["Hello", "Nexus", "World"]
    data_log = "ERROR: Connection timeout"
    processor_num = NumericProcessor()
    processor_str = TextProcessor()
    processor_log = LogProcessor()
    processors_instances = [processor_num, processor_str, processor_log]
    data_list = [data_num, data_str, data_log]
    for processor, data in zip(processors_instances, data_list):
        print(processor.process(data))
    print("=== Polymorphic Processing Demo ===")


if __name__ == "__main__":
    main()
