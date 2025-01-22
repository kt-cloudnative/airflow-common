import json
import random
from dataclasses import dataclass
from datetime import datetime


@dataclass()
class CommonHeader:
    appName: str #@Schema(description = "어플리케이션이름", nullable = false) 
    svcName: str #@Schema(description = "서비스이름", nullable = false)
    fnName: str #@Schema(description = "오퍼레이션이름", nullable = false)
    fnCd: str #@Schema(description = "기능코드", nullable = true)
    globalNo: str # @Schema(description = "거래고유번호", nullable = false)
    chnlType: str #@Schema(description = "채널구분", nullable = false)
    envrFlag: str #@Schema(description = "환경구분", nullable = true, example = "사용하지 않음")
    trFlag: str # @Schema(description = "송수신FLAG", nullable = true, example = "T/R")
    trDate: str #@Schema(description = "송수신일자", nullable = false, example = "20080609(2008년 6월 9일)")
    trTime: str #@Schema(description = "송수신시간", nullable = false, example = "235531005(23시 55분 31.005초))")
    clntIp: str #@Schema(description = "클라이언트ip", nullable = false)
    responseType: str #@Schema(description = "응답유형", nullable = false, example = "S/E/I/D")
    responseCode: str #@Schema(description = "응답코드", nullable = false, example = "COME5501,공백")
    responseLogcd: str #@Schema(description = "응답구분코드", nullable = true)
    responseTitle: str #@Schema(description = "응답타이틀", nullable = false)
    responseBasc: str #@Schema(description = "응답기본내역", nullable = false)
    responseDtal: str #@Schema(description = "응답상세내역", nullable = false)
    responseSystem: str # @Schema(description = "응답시스템", nullable = true, example = "1")
    userId: str #@Schema(description = "사용자아이디", nullable = false, example = "통합ID에서 사용")
    realUserId: str #@Schema(description = "실사용자아이디", nullable = true, example = "채널ID에서 사용")
    filler: str #@Schema(description = "필러", nullable = true, example = "EAI에서요청시 DTO의 풀명칭")
    langCode: str # @Schema(description = "사용자언어코드", nullable = true, example = "사용하지 않음")
    orgId: str #@Schema(description = "조직ID", nullable = false, example = "사용자가 속한 최하부 조직ID")
    srcId: str #@Schema(description = "Source ID", nullable = false, example = "최초 이벤트 발생 Program ID")
    curHostId: str #@Schema(description = "Current Host ID", nullable = true, example = "현재 처리 hostname")
    lgDateTime: str #@Schema(description = "Logical Date & Time", nullable = false, example = "20080609221020 (2008년 6월 9일 22시 10분 20초)")
    tokenId: str #@Schema(description = "Token Id", nullable = true, example = "ESB->권한/보안을 통해 얻은 토큰ID")
    cmpnCd: str #@Schema(description = "Company Code", nullable = false, example = "판매회사코드")
    lockType: str #@Schema(description = "Locking 유형", nullable = true, example = "\"1\": ban전용, \"2\": ncn전용, \"3\": ban+ncn")
    lockId: str # @Schema(description = "Locking ID", nullable = true)
    lockTimeSt: str #@Schema(description = "Locking Timestamp", nullable = true)
    businessKey: str #@Schema(description = "비즈니스키", nullable = true, example = "E2E 모니터링 비즈니스 키 전달 용도")
    arbitraryKey: str #@Schema(description = "임의키", nullable = true, example = "Reserved필드")
    resendFlag: str #@Schema(description = "재처리연동구분", nullable = true, example = "1")
    phase: str #@Schema(description = "phase", nullable = true, example = "N/R/C")
    logPoint: str #@Schema(description = "B-MON 로그포인트", nullable = false, example = "EB/IC/... BMON 담장자로부터 전달받은 로그포인트 ")

def getGlobalNo(user_id):
        return (user_id+""+datetime.today().strftime('%Y%m%d%H%M%S')+""+str(random.randint(100000,999999))).zfill(32)

data1 = CommonHeader(
    appName="NBSS_TORD", #AIRFLOW?
    fnName="fnName11111",
    fnCd="",
    envrFlag="",
    responseType="",
    responseCode="",
    responseLogcd="",
    responseTitle="",
    responseBasc="",
    responseDtal="",
    responseSystem="",
    langCode="",
    tokenId="",
    lockType="",
    lockId="",
    lockTimeSt="",
    businessKey="",
    arbitraryKey="",
    resendFlag="",
    phase="",
    logPoint="",
    svcName="svsvsvsvcname",
    globalNo=getGlobalNo("82258624"),
    chnlType="IC",
    trFlag="T",
    trDate=datetime.today().strftime('%Y%m%d'),
    trTime=datetime.today().strftime('%H%M%S'),
    clntIp="192.168.0.1",
    userId="82258624",
    realUserId="82258624",
    filler="",
    orgId="SPT8050",
    srcId="SWAGGER",
    curHostId="ICS_TR_01",
    lgDateTime=datetime.today().strftime('%Y%m%d%H%M%S'),
    cmpnCd="KT"
)

data2 = json.dumps(data1.__dict__)
print(data1)
print(data2)