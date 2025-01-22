from dataclasses import dataclass, asdict
from json import dumps

@dataclass()
class BizHeader:
    orderId: str #@Schema(description = "Order ID", nullable = true)
    cbSvcName: str #@Schema(description = "CB서비스이름", nullable = false)
    cbFnName: str #@Schema(description = "CB오퍼레이션 이름", nullable = false)

    def toDict(self):
        return asdict(self)

    def toJson(self):
        return dumps(asdict(self), ensure_ascii=False)