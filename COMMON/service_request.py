from dataclasses import dataclass, asdict
from json import dumps

import biz_header
import common_header
import sa_security


@dataclass()
class serviceRequest:
    commonHeader: common_header
    bizHeader: biz_header
    saSecurity: sa_security

    def toDict(self):
        return asdict(self)

    def toJson(self):
        return dumps(asdict(self), ensure_ascii=False)