from dataclasses import dataclass

@dataclass()
class ICISLog:
    SERVICE: str #서비스명,
    TYPE: str #유형(ONLINE|BATCH),
    TRACE: str #TRANS_ID:SPAN_ID,
    CATEGORY: str #분류(DEBUG|MON|...)
    DATE: str #yyyyMMddHHmmssSSS,
    SOURCE: str #클래스명.메소드명(라인),
    LOGLEVEL: str #로그 레벨 표기
    HEADER: str #공통헤더,
    MESSAGE: str #내용
    