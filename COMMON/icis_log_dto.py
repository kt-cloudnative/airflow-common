from dataclasses import dataclass,asdict
from json import dumps

@dataclass()
class ICISLog:
    SERVICE: str #서비스명,
    TYPE: str #유형(ONLINE|BATCH),
    CATEGORY: str #분류(DEBUG|MON|...)
    WORKFLOW: str
    GLOBAL_NO: str
    TOPIC_NAME: str
    TOPOLOGY_SEQ: str
    TRANSACTION_ID : str
    TRACE_ID: str
    ERROR_ID: str
    DATE: str #yyyyMMddHHmmssSSS,
    SOURCE: str #클래스명.메소드명(라인),
    LOG_LEVEL: str #로그 레벨 표기
    HEADER: str
    MESSAGE: str #내용

    def toDict(self):
        return asdict(self)

    def toJson(self):
        return dumps(asdict(self), ensure_ascii=False)