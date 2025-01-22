from dataclasses import dataclass

@dataclass()
class BizHeader:
    orderId: str #@Schema(description = "Order ID", nullable = true)
    cbSvcName: str #@Schema(description = "CB서비스이름", nullable = false)
    cbFnName: str #@Schema(description = "CB오퍼레이션 이름", nullable = false)
    
#data1 = BizHeader(
#    orderId="",
#    cbSvcName="/ppon/syscd/retvByGrpId",
#    cbFnName="service"
#)

#data2 = json.dumps(data1.__dict__)
#print(data1)
#print(data2)