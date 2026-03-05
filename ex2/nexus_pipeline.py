from abc import ABC
from typing import Any, Union, Dict, Protocol

json_file = {"sensor": "temp", "value": 23.5, "unit": "C"}
csv_file = "user,action,timestamp"
stream_data = "Real-time sensor stream"


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id) -> None:
        self.pipeline_id = pipeline_id
        self.stages = []
        self.file = None
        self.type = ""

    def add_stage(self, stage):
        self.stages.append(stage)

    def process(self, data: Any) -> Any:
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())
        for stage in self.stages:
            stage.process(data)
        return "je verrais plus tard pour retourner un truc , surement ok ko"


class NexusManager():

    def __init__(self) -> None:
        self.pipelines: list[ProcessingPipeline] = []
        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second\n")

    def add_pipeline(self, pipeline: ProcessingPipeline):
        self.pipelines.append(pipeline)

    def process_data(self):
        for adapter in self.pipelines:
            adapter.process(adapter)


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        print(data)


class InputStage():
    def process(self, data: Any) -> Dict:
        match data.type:
            case "json":
                print("Processing JSON data through pipeline...")
                print(f"Input: {data.file}")
            case "csv":
                print("Processing CSV data through same pipeline...")
                print(f'Input: "{data.file}"')
            case "stream":
                print("Processing Stream data through same pipeline...")
        return data.file


class TransformStage():
    def process(self, data: Any) -> Dict:
        match data.type:
            case "json":
                data.file.update({'extra': 'normal range'})
                print("Transform: Enriched with metadata and validation")
            case "csv":
                word_list = data.file.split(',')
                data.action = len([value for value in word_list
                                   if value == "action"])
                print("Transform: Parsed and structured data")
            case "stream":
                print("Processing Stream data through same pipeline...")
        return data.file


class OutputStage():
    def process(self, data: Any) -> str:
        match data.type:
            case "json":
                print("Output: Processed temperature reading:"
                      f" {data.file['value']}°{data.file['unit']}"
                      f" ({data.file['extra']})\n")
            case "csv":
                print(f"Output: User activity logged: {data.action}"
                      " actions processed")
            case "stream":
                print("Processing Stream data through same pipeline...\n")
        return data.file


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id, file: dict) -> None:
        super().__init__(pipeline_id)
        self.file = file
        self.type = "json"

    # def process(self, data: Any) -> Union[str, Any]:
    #     pass


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id, file: str) -> None:
        super().__init__(pipeline_id)
        self.file = file
        self.action = 0
        self.type = "csv"

    # def process(self, data: Any) -> Union[str, Any]:
    #     pass


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id) -> None:
        self.pipeline_id = pipeline_id
        self.format = "stream"

    def process(self, data: Any) -> Union[str, Any]:
        pass


def main():
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    json = JSONAdapter("JSON_001", json_file)
    csv = CSVAdapter("JSON_001", csv_file)
    nexus = NexusManager()
    nexus.add_pipeline(json)
    nexus.add_pipeline(csv)
    print("Creating Data Processing Pipeline...\n"
          "Stage 1: Input validation and parsing\n"
          "Stage 2: Data transformation and enrichment\n"
          "Stage 3: Output formatting and delivery\n")
    print("=== Multi-Format Data Processing ===\n")
    nexus.process_data()
    # nexus.process_data(csv)


if __name__ == "__main__":
    main()
