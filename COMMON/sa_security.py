from dataclasses import dataclass, asdict
from json import dumps

@dataclass()
class SaSecurity:
    payloadOrigin: str
    env: str
    unLockSkip: str
    lockClearYn: str
    topologySeq: str
    compositonCnt: int

    def toDict(self):
        return asdict(self)

    def toJson(self):
        return dumps(asdict(self), ensure_ascii=False)