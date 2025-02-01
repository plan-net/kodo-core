from kodo.datatypes import DynamicModel


from typing import Any


class ResultFormatter:

    def __init__(self):
        pass

    def br(self, s: str) -> str:
        return s.replace("\n", "<br/>")

    def format(self, event: dict) -> str:
        if event["event"] in ("result", "final"):
            if ": " in event["data"]:
                timestamp, data = event["data"].split(": ", 1)
                record = DynamicModel.model_validate_json(data)
                kind = list(record.root.keys())[0]
                return getattr(self, kind)(
                    list(record.root.values())[0])
        return self.default(event)

    def AgentAction(self, data: dict) -> str:
        tool = data.get("tool", None)
        text = data.get("text", None)
        result = data.get("result", None)
        return f"<h4>{tool}</h4><p>{self.br(text)}</p><p>{self.br(result)}</p>"

    def AgentFinish(self, data: dict) -> str:
        thought = data.get("thought", None)
        output = data.get("output", None)
        return f"<h4>Thought</h4><p>{thought}</p><p>{self.br(output)}</p><p>"

    def CrewOutput(self, data: dict) -> str:
        raw = data.get("raw", None)
        return f"<h4>Crew Result</h4><p>{self.br(raw)}</p><p>"

    def TaskOutput(self, data: dict) -> str:
        name = data.get("name", None)
        raw = data.get("raw", None)
        return f"<h4>{name}</h4><p>{self.br(raw)}</p><p>"

    def default(self, data: Any) -> str:
        return f'<i class="console-bg">{data}</i><br/>'

    def __getattr__(self, name: str):
        return self.default