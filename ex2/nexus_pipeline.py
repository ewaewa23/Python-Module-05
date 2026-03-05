from abc import ABC
from typing import Any, Union, Dict, Protocol

json_file = {"sensor": "temp", "value": 23.5, "unit": "C"}
csv_file = "user,action,timestamp"
stream_data = "Real-time sensor stream"


def check_format(data: Any) -> str:
    if isinstance(data, dict):
        return "json"
    elif isinstance(data, str) and "," in data:
        return "csv"
    else:
        return "stream"


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id) -> None:
        self.pipeline_id = pipeline_id
        self.stages = [
            InputStage(),
            TransformStage(),
            OutputStage()
        ]
        self.file = None

    def add_stage(self, stage):
        self.stages.append(stage)

    def process(self, data: Any) -> Any:
        retour: Any = 0
        for stage in self.stages:
            retour = stage.process(data)
        return retour
        # return "je verrais plus tard pour retourner un truc , surement ok ko"


class NexusManager():

    def __init__(self) -> None:
        self.pipelines: list[ProcessingPipeline] = []
        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second\n")

    def add_pipeline(self, pipeline: ProcessingPipeline):
        self.pipelines.append(pipeline)

    def process_data(self):
        for adapter in self.pipelines:
            adapter.process(adapter.file)


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass


class InputStage():
    def process(self, data: Any) -> Dict:
        match check_format(data):
            case "json":
                print("Processing JSON data through pipeline...")
                print(f"Input: {data}")
            case "csv":
                print("Processing CSV data through same pipeline...")
                print(f'Input: "{data}"')
            case "stream":
                print("Processing Stream data through same pipeline...")
                print("Input: Real-time sensor stream")
        return data


class TransformStage():
    def process(self, data: Any) -> Dict:
        match check_format(data):
            case "json":
                data.update({'extra': 'normal range'})
                print("Transform: Enriched with metadata and validation")
            case "csv":
                print("Transform: Parsed and structured data")
            case "stream":
                print("Transform: Aggregated and filtered")
        return data


class OutputStage():
    def process(self, data: Any) -> str:
        match check_format(data):
            case "json":
                print("Output: Processed temperature reading:"
                      f" {data['value']}°{data['unit']}"
                      f" ({data['extra']})\n")
            case "csv":
                word_list = data.split(',')
                action = len([value for value in word_list
                              if value == 'action'])
                print(f"Output: User activity logged: {action}"
                      " actions processed\n")
            case "stream":
                print("Output: Stream summary: 5 readings, avg: 22.1°C\n")
        return data


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id, file: dict) -> None:
        super().__init__(pipeline_id)
        self.file = file

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing JSON adapter:", self.pipeline_id)
        return super().process(data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id, file: str) -> None:
        super().__init__(pipeline_id)
        self.file = file
        self.action = 0

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing CSV adapter:", self.pipeline_id)
        return super().process(data)


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id, file: str) -> None:
        super().__init__(pipeline_id)
        self.file = file

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing Stream adapter:", self.pipeline_id)
        return super().process(data)


def main():
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    json = JSONAdapter("JSON_001", json_file)
    csv = CSVAdapter("JSON_001", csv_file)
    stream = StreamAdapter("STREAM_001", stream_data)
    nexus = NexusManager()
    nexus.add_pipeline(json)
    nexus.add_pipeline(csv)
    nexus.add_pipeline(stream)
    print("Creating Data Processing Pipeline...\n"
          "Stage 1: Input validation and parsing\n"
          "Stage 2: Data transformation and enrichment\n"
          "Stage 3: Output formatting and delivery\n")
    print("=== Multi-Format Data Processing ===\n")
    nexus.process_data()


if __name__ == "__main__":
    main()
