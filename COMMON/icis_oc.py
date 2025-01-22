from __future__ import annotations

import math
import random
import sys
import time
from datetime import timedelta
from functools import partial
from typing import Union, List, Tuple
from urllib.parse import urlparse

from airflow.models import BaseOperator
from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

sys.path.append("/opt/bitnami/airflow/dags/git_sa-common") #common repo dir in cluster
import kubernetes_pod
import pendulum
import json

import requests
import biz_header
import uuid
import base64
import common_header
import icis_log_dto
import service_request
import sa_security
import logging
from icis_util import DataUtil

REAL_TIME = pendulum.now("Asia/Seoul")
TaskType = Union[BaseOperator, Tuple[BaseOperator, Union[BaseOperator, Tuple], Union[BaseOperator, Tuple]]]
NestedTaskType = Union[TaskType, List[TaskType]]

NEXUS_URL="nexus.dspace.kt.co.kr/"
DEVPILOT_SVC_URL = "icis-sa-devpilot-backend.devpilot.svc/api/v1"

def auth_check(response):
    try:
        isAuth = response.json().get("data",False)
        if isAuth is True:
            logging.info("[CTG:CMMN] auth_check > This workflow is authorized")
            return True
        else:
            raise Exception("[CTG:CMMN] auth_check > This workflow is not authorized")
    except ValueError:
        raise Exception("[CTG:CMMN] auth_check > This workflow is not authorized")

def getICISConfigMap(configmap_name):
    return k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap_name))

def getICISSecret(secret_name):
    return k8s.V1EnvFromSource(secret_ref= k8s.V1SecretEnvSource(name=secret_name))

def getVolume(volume_name,pvc_name):
    return k8s.V1Volume(name=volume_name, persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name))

def getVolumeMount(volume_name,m_path):
    return k8s.V1VolumeMount(name=volume_name, mount_path=m_path, sub_path=None, read_only=False)

def getAllVolumes(prm):
    volumes = [getICISVolumeJks()]
    if "volumes" in prm:
        for list in prm["volumes"]:
            volumes.append(list)
    return volumes

def getAllVolumeMount(prm):
    volume_mounts = [getICISVolumeMountJks()]
    if "volume_mounts" in prm:
        for list in prm["volume_mounts"]:
            volume_mounts.append(list)
    return volume_mounts

def getICISVolumeJks():
    return k8s.V1Volume(name="truststore-jks", config_map=k8s.V1ConfigMapVolumeSource(name="truststore.jks"))

def getICISVolumeMountJks():
    return k8s.V1VolumeMount(name="truststore-jks", mount_path="/app/resources/truststore.jks", sub_path="truststore.jks", read_only=False)

def getICISHostAliases():
    return k8s.V1HostAlias(ip="10.217.137.12",hostnames=["sa-cluster-kafka-0.sit.icis.kt.co.kr","sa-cluster-kafka-1.sit.icis.kt.co.kr","sa-cluster-kafka-2.sit.icis.kt.co.kr"])

#오류처리필요
def getAccess_control(DOMAIN):
    return {
        "icis-"+DOMAIN+"_user":{"can_dag_read"},
        "icis-"+DOMAIN+"_admin":{"can_dag_read","can_dag_edit"}
    }

def getGlobalNo(user_id):
    return (user_id+""+REAL_TIME.strftime("%Y%m%d%H%M%S")+""+str(random.randint(100000,999999))).zfill(32)

class CustomSimpleHttpOperator(SimpleHttpOperator):
    @apply_defaults
    def __init__(self, custom_self='', custom_prm={}, *args, **kwargs):
        super(CustomSimpleHttpOperator, self).__init__(*args, **kwargs)
        self.custom_self = custom_self
        self.custom_prm = custom_prm

    def pre_execute(self, context):
        logging.info(f"[CTG:CMMN] CustomSimpleHttpOperator.pre_execute > Task:[{self.task_id}]")

        if self.custom_prm.get("taskAlrmStYn", "N") == "Y":
            ICISCmmn.startTaskAlrm(self.custom_self, self.custom_prm)

    def execute(self, context):
        # HTTP 요청 수행
        logging.info(f"[CTG:CMMN] CustomSimpleHttpOperator.execute > Task:[{self.task_id}], endpoint: [{self.endpoint}], data: [{self.data}], header: [{self.headers}]")

        ICISCmmn.getICISLog(self.custom_self, self.custom_prm, "MON")

        return super().execute(context)

    def post_execute(self, context, result=None):
        logging.info(f"[CTG:CMMN] CustomSimpleHttpOperator.post_execute > Task:[{self.task_id}]")

        if self.custom_prm.get("taskAlrmFnsYn", "N") == "Y":
            ICISCmmn.endTaskAlrm(self.custom_self, self.custom_prm)

class CustomKubernetesPodOperator(kubernetes_pod.KubernetesPodOperator):
    @apply_defaults
    def __init__(self, custom_self='', custom_prm={},  *args, **kwargs):
        super(CustomKubernetesPodOperator, self).__init__(*args, **kwargs)
        self.custom_self = custom_self
        self.custom_prm = custom_prm

    def pre_execute(self, context):
        logging.info(f"[CTG:CMMN] CustomKubernetesPodOperator.pre_execute > Task:[{self.task_id}]")

        if self.custom_prm.get("taskAlrmStYn", "N") == "Y":
            ICISCmmn.startTaskAlrm(self.custom_self, self.custom_prm)

    def execute(self, context):
        logging.info(f"[CTG:CMMN] CustomKubernetesPodOperator.execute > Task:[{self.task_id}], image: [{self.image}], arguments: [{self.arguments}], "
                     f"container_resources:[{self.container_resources}], security_context: [{self.security_context}]")

        ICISCmmn.getICISLog(self.custom_self, self.custom_prm, "MON")

        return super().execute(context)

    def post_execute(self, context, result=None):
        logging.info(f"[CTG:CMMN] CustomKubernetesPodOperator.post_execute > Task:[{self.task_id}]")

        if self.custom_prm.get("taskAlrmFnsYn", "N") == "Y":
            ICISCmmn.endTaskAlrm(self.custom_self, self.custom_prm)

class ICISCmmn():
    LOG_LEVEL=""
    DOMAIN=""
    ENV=""
    NAMESPACE=""

    WORKFLOW_ID=""
    WORKFLOW_NAME=""

    GLOBAL_NO=""
    APP_NAME=""
    CHNL_TYPE=""
    USER_ID=""

    def __init__(self, DOMAIN, ENV, NAMESPACE, WORKFLOW_ID=None, WORKFLOW_NAME=None, APP_NAME=None, CHNL_TYPE=None, USER_ID=None, LOG_LEVEL=None):
        self.LOG_LEVEL = LOG_LEVEL or "INFO"
        level = logging.getLevelName(self.LOG_LEVEL)
        logging.basicConfig(level=level)

        self.ENV = ENV
        self.DOMAIN = DOMAIN
        self.NAMESPACE = NAMESPACE

        self.WORKFLOW_ID = WORKFLOW_ID or ""
        self.WORKFLOW_NAME = WORKFLOW_NAME or ""

        self.APP_NAME = APP_NAME or ICISCmmn.getAppName(self)
        self.CHNL_TYPE = CHNL_TYPE or ICISCmmn.getChnlType(self)
        self.USER_ID = USER_ID or ICISCmmn.getUserId(self)
        self.GLOBAL_NO = getGlobalNo(self.USER_ID)


    def __enter__(self):
        # 사용할 자원을 가져오거나 만든다(핸들러 등)
        logging.info("[CTG:CMMN] ICISCmmn > enter...")
        return self # 반환값이 있어야 VARIABLE를 블록내에서 사용할 수 있다

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 마지막 처리를 한다(자원반납 등)
        logging.info("[CTG:CMMN] ICISCmmn > exit...")

    def getICISDAG(self,prm):
        self.WORKFLOW_NAME = prm["dag_id"]
        prm["task_id"] = prm["dag_id"]
        ICISCmmn.getICISLog(self,prm)

        return DAG(
            access_control=getAccess_control(self.DOMAIN),
            default_args={
                "owner": self.DOMAIN,
                "depends_on_past": False,
                "retries": prm.get("retries", 0),
                "retry_delay": prm.get("retry_delay", timedelta(minutes=5))
            },
            tags=[self.DOMAIN,self.ENV],
            max_active_runs=prm.get("max_active_runs", 16),
            catchup=False,#backfill
            # dag_id=prm["dag_id"]+"-"+self.ENV, #airflow에 보여질 dag_id 중복불가
            dag_id=prm["dag_id"],
            schedule_interval=prm["schedule_interval"], # scheduling
            start_date=prm["start_date"],
            end_date=prm.get("end_date", None), # scheduling TO DO 수정필요
            is_paused_upon_creation=prm.get("paused", False)
        )

    ################################ [TASK 영역] ################################

    def getICISCompleteWflowTask(self, wflow_id):
        return PythonOperator(task_id="ICIS_CompleteWflow", python_callable = partial(ICISCmmn.success_callback,self, wflow_id, "ICIS_CompleteWflow"), trigger_rule = TriggerRule.ALL_SUCCESS)

    def getICISAuthCheckWflow(self, wflow_id):
        ICISCmmn.getICISLog(self, {"task_id": "ICIS_AuthCheckWflow"})
        if wflow_id is None:
            raise Exception("[CTG:CMMN] ICIS_AuthCheckWflow > Workflow id not found")

        return EmptyOperator(task_id = "ICIS_AuthCheckWflow")

    def strToBool(self):
        if self in ["TRUE","True","true", "1"]:
            return True
        else:
            return False

    def getICISKubernetesPodOperator_v1(self,prm):
        repoaddr = Variable.get("repoaddr")
        cluster = Variable.get("cluster")
        context = Variable.get("context")
        config = Variable.get("config")
        path = prm["image"]
        image = f"{repoaddr}{path}"

        arguments = DataUtil.process_template(prm.get("arguments", []))

        return CustomKubernetesPodOperator(
            in_cluster=ICISCmmn.strToBool(cluster),
            cluster_context=context,
            config_file=config,
            volumes=getAllVolumes(prm),
            volume_mounts=getAllVolumeMount(prm),
            env_from=prm["env_from"], #환경별 정보 (configmap,secret)
            task_id=prm["task_id"],
            image=image,
            arguments=arguments,
            namespace=self.NAMESPACE,
            get_logs=True,
            image_pull_secrets = [k8s.V1LocalObjectReference("dspace-nexus")],
            name=prm["task_id"],
            is_delete_operator_pod = True,
            env_vars=ICISCmmn.ICISCommonEnvVars(self,prm),
            host_aliases=[getICISHostAliases()],
            on_failure_callback = partial(ICISCmmn.failure_callback, self, prm.get("id",""), prm.get("task_id","")),
            trigger_rule= prm.get("trigger_rule", TriggerRule.ALL_SUCCESS),
            custom_self=self,
            custom_prm=prm,
            container_resources=ICISCmmn.getContainerResources(self, prm),
            security_context=ICISCmmn.getSecurityContext(self, prm)
        )

    def getICISSimpleHttpOperator_v1(self,prm):
        headers = ICISCmmn.getHttpheader(self,prm)
        headers.update(prm.get("headers", {}))

        endpoint = prm.get("endpoint", "")
        data = DataUtil.process_template(prm.get("data", {}))

        if "rest" in endpoint.lower():
            endpoint = urlparse("http://"+prm.get("endpoint", "")).netloc
            # endpoint = urlparse("http://"+prm.get("endpoint", "")).netloc + "/json"
            # data.update(ICISCmmn.getHttpExternalheader(self,prm))

        return CustomSimpleHttpOperator(
            headers=headers,
            task_id=prm["task_id"],
            method=prm.get("method", "POST"),
            http_conn_id="http_default",
            data= json.dumps(data),
            endpoint=endpoint,
            log_response=True,
            response_filter= lambda res: json.loads(res.text),
            on_failure_callback = partial(ICISCmmn.failure_callback, self, prm.get("id",""), prm.get("task_id","")),
            trigger_rule= prm["trigger_rule"] if "trigger_rule" in prm else TriggerRule.ALL_SUCCESS,
            custom_self=self,
            custom_prm=prm
        )

    def getAgentHttpOperator(self,prm):
        return PythonOperator(
            task_id=prm["task_id"],
            python_callable=ICISCmmn.call_task_api,
            op_kwargs={
                "self": self,
                "url": "{}/batch-commander/exe/exeHttp".format(prm["endpoint"]),
                "prm": prm
            },
            on_failure_callback = partial(ICISCmmn.failure_callback, self, prm.get("id",""), prm.get("task_id","")),
            trigger_rule= prm["trigger_rule"] if "trigger_rule" in prm else TriggerRule.ALL_SUCCESS
        )

    def getAgentShellOperator(self,prm):
        return PythonOperator(
            task_id=prm["task_id"],
            python_callable=ICISCmmn.call_task_api,
            op_kwargs={
                "self": self,
                "url": "{}/batch-commander/exe/exeShell".format(prm["endpoint"]),
                "prm": prm
            },
            on_failure_callback = partial(ICISCmmn.failure_callback, self, prm.get("id",""), prm.get("task_id","")),
            trigger_rule= prm["trigger_rule"] if "trigger_rule" in prm else TriggerRule.ALL_SUCCESS
        )

    def getAgentVrfOperator(self,prm):
        return PythonOperator(
            task_id=prm["task_id"],
            python_callable=ICISCmmn.call_vrf_api,
            op_kwargs={
                "self": self,
                "url": "{}/batch-commander/vrf/resultVrfTask".format(prm["endpoint"]),
                "prm": prm
            },
            on_failure_callback = partial(ICISCmmn.failure_callback, self, prm.get("id",""), prm.get("task_id","")),
            trigger_rule= prm["trigger_rule"] if "trigger_rule" in prm else TriggerRule.ALL_SUCCESS
        )

    def call_task_api(self, url, prm):
        try:

            if prm.get("taskAlrmStYn", "N") == "Y":
                ICISCmmn.startTaskAlrm(self,prm)

            headers = ICISCmmn.getHttpheader(self,prm)
            headers.update(prm.get("headers", {}))

            data = DataUtil.process_template(prm.get("data", {}))
            logging.info(f"[CTG:CMMN] call_task_api > Task:[{prm.get('task_id', '')}], endpoint: [{url}], data: [{data}], header: [{headers}]")
            ICISCmmn.getICISLog(self,prm,"MON")

            # HttpHook 초기화
            hook = HttpHook(method=prm.get("method", "POST"), http_conn_id="http_default")
            response = hook.run(endpoint=url, json=data, headers=headers)

            logging.info(f"[CTG:CMMN] call_task_api > Response: {response.text}")

            if response.ok:
                result = response.json()
                jobReslt = result.get("obj", {}).get("jobReslt")
                rcvJsonMotSbst = result.get("obj", {}).get("rcvJsonMotSbst")

                if jobReslt == "S":
                    logging.info(f"[CTG:CMMN] call_task_api > API call was successful. / rcvJsonMotSbst: {rcvJsonMotSbst}")

                elif jobReslt == "F":
                    raise ValueError(f"[CTG:CMMN] call_task_api > API call returned False, marking this task as failed. / rcvJsonMotSbst: {rcvJsonMotSbst}")

                else:
                    raise ValueError(f"[CTG:CMMN] call_task_api > Batch Agent > call_task_api / jobReslt: {jobReslt}, rcvJsonMotSbst: {rcvJsonMotSbst}")

            else:
                raise ValueError(f"[CTG:CMMN] call_task_api > API call failed with status code {response.status_code}")

        finally:
            if prm.get("taskAlrmFnsYn", "N") == "Y":
                ICISCmmn.endTaskAlrm(self,prm)
            logging.info("[CTG:CMMN] call_task_api > API call was finally.")

    def call_vrf_api(self, url, prm):
        try:

            headers = ICISCmmn.getHttpheader(self,prm)
            headers.update(prm.get("headers", {}))

            data = DataUtil.process_template(prm.get("data", {}))
            logging.info(f"[CTG:CMMN] call_vrf_api > Task:[{prm.get('task_id', '')}], endpoint: [{url}], data: [{data}], header: [{headers}]")
            ICISCmmn.getICISLog(self,prm,"MON")

            # HttpHook 초기화
            hook = HttpHook(method="POST", http_conn_id="http_default")
            response = hook.run(endpoint=url, json=data, headers=headers)


            logging.info(f"[CTG:CMMN] call_vrf_api > Response: {response.text}")

            if response.ok:
                result = response.json()
                vrfReslt = result.get("obj", {}).get("vrfReslt")
                rcvJsonMotSbst = result.get("obj", {}).get("rcvJsonMotSbst")

                if vrfReslt == "S":
                    if prm.get("taskAlrmSucesYn", "N") == "Y":
                        ICISCmmn.vrfSuccessAlrm(self,prm)
                    logging.info(f"[CTG:CMMN] call_vrf_api > API call was successful. / rcvJsonMotSbst: {rcvJsonMotSbst}")

                elif vrfReslt == "F":
                    if prm.get("taskAlrmFailYn", "N") == "Y":
                        ICISCmmn.vrfFailureAlrm(self,prm)
                    raise ValueError(f"[CTG:CMMN] call_vrf_api > API call returned False, marking this task as failed. / rcvJsonMotSbst: {rcvJsonMotSbst}")

                else:
                    raise ValueError(f"[CTG:CMMN] call_vrf_api > Batch Agent > call_vrf_api / jobReslt: {vrfReslt}, rcvJsonMotSbst: {rcvJsonMotSbst}")

            else:
                raise ValueError(f"[CTG:CMMN] call_vrf_api > API call failed with status code {response.status_code}")

        finally:
            logging.info("[CTG:CMMN] call_vrf_api > API call was finally.")

    def getICISEmptyOperator(self,prm):
        return EmptyOperator(task_id=prm["task_id"])


    ################################ [공통함수 영역] ################################
    def ICISCommonEnvVars(self,prm):
        return [
            {
                "name": "NODE_IP",
                "valueFrom": {
                    "fieldRef": {
                        "apiVersion": "v1",
                        "fieldPath": "status.hostIP"
                    }
                }
            },{
                "name": "POD_IP",
                "valueFrom": {
                    "fieldRef": {
                        "apiVersion": "v1",
                        "fieldPath": "status.podIP"
                    }
                }
            },{
                "name": "NODE_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "apiVersion": "v1",
                        "fieldPath": "spec.nodeName"
                    }
                }
            },{
                "name": "POD_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "apiVersion": "v1",
                        "fieldPath": "metadata.name"
                    }
                }
            },{
                "name": "ZONE",
                "valueFrom": {
                    "configMapKeyRef": {
                        "name": "zone-config",
                        "key": "zone"
                    }
                }
            },{
                "name": "CMMN_HEADER_ORIGIN",#평문
                "value": json.dumps(ICISCmmn.getCommonHeader(self,prm))
            },{
                "name": "CMMN_HEADER",#암호화
                "value": base64.b64encode(json.dumps(ICISCmmn.getCommonHeader(self,prm)).encode("utf-8")).decode("ascii") #base64 Encode
            },{
                "name": "WORKFLOW_NAME",
                "value": self.WORKFLOW_NAME
            },{
                "name": "GLOBAL_NO",
                "value": self.GLOBAL_NO
            }
        ]

    def mkHeader(self,prm):
        if "data" in prm:
            header = {
                "service_request": {
                    "payload": prm["data"],
                    "commonHeader": ICISCmmn.getCommonHeader(self,prm),
                    "bizHeader": ICISCmmn.getBizHeader(self, prm),
                    "saSecurity": ICISCmmn.getSaSecurity(self,prm)
                }
            }
            return json.dumps(header)
        else:
            header = {
                "service_request": {
                    "commonHeader": ICISCmmn.getCommonHeader(self,prm),
                    "bizHeader": ICISCmmn.getBizHeader(self, prm),
                    "saSecurity": ICISCmmn.getSaSecurity(self,prm)
                }
            }
            return json.dumps(header)

    def getServiceRequest(self,prm):
        return service_request.serviceRequest(
            commonHeader=ICISCmmn.getCommonHeader(self,prm),
            bizHeader=ICISCmmn.getBizHeader(self,prm),
            saSecurity=ICISCmmn.getSaSecurity(self,prm)
        ).toJson()

    def getBizHeader(self, prm):
        return biz_header.BizHeader(
            orderId="",
            cbSvcName=urlparse("http://"+prm.get("endpoint", "")).path,
            cbFnName="service"
        ).toDict()

    def getSaSecurity(self, prm):
        return sa_security.SaSecurity(
            payloadOrigin="F",
            env="prd" if "prd" in str(self.ENV).lower() else str(self.ENV).lower(),
            unLockSkip=None,
            lockClearYn=None,
            topologySeq="1",
            compositonCnt=1
        ).toDict()

    def getCommonHeader(self,prm):
        return common_header.CommonHeader(
            appName=self.APP_NAME,
            svcName=urlparse("http://"+prm.get("endpoint", "")).path,
            fnName=prm.get("task_id", "common_header"),
            fnCd="",
            globalNo=self.GLOBAL_NO,
            chnlType=self.CHNL_TYPE,
            envrFlag="L",
            trFlag="T",
            trDate=REAL_TIME.strftime("%Y%m%d"),
            trTime=REAL_TIME.strftime("%H%M%S") + f"{REAL_TIME.microsecond:06d}"[:3],
            clntIp="192.168.0.1",
            responseType="",
            responseCode="",
            responseLogcd="",
            responseTitle="",
            responseBasc="",
            responseDtal="",
            responseSystem="",
            userId=self.USER_ID,
            realUserId="82258624",
            filler="",
            langCode="",
            orgId="SPT8050",
            srcId="airflow/"+prm.get("task_id", "task_id"),
            curHostId="ICS_TR_01",
            lgDateTime=REAL_TIME.strftime("%Y%m%d%H%M%S"),
            tokenId="",
            cmpnCd="KT",
            lockType="",
            lockId="",
            lockTimeSt="",
            businessKey="",
            arbitraryKey="",
            resendFlag="",
            phase=""
        ).toDict()

    def getICISLog(self, prm, logType=None):
        logging.info(icis_log_dto.ICISLog(
            SERVICE="AIRFLOW",
            TYPE="airflow",
            CATEGORY=logType or "CMMN",
            WORKFLOW=self.WORKFLOW_NAME,
            GLOBAL_NO=self.GLOBAL_NO,
            TOPIC_NAME= "",
            TOPOLOGY_SEQ= "1",
            TRANSACTION_ID=prm.get("task_id", "DAG"),
            TRACE_ID=str(uuid.uuid1())+":0",
            ERROR_ID="",
            DATE=REAL_TIME.strftime("%Y%m%d%H%M%S"),
            SOURCE=prm.get("task_id", "DAG"),
            LOG_LEVEL=self.LOG_LEVEL,
            MESSAGE= "" #내용
        ).toJson())

    def getHttpheader(self,prm):

        if "rest" in prm.get("endpoint", "").lower():
            header = {
                "Content-Type": "application/json",
                "Req-Common-Header":base64.b64encode(ICISCmmn.getServiceRequest(self,prm).encode("utf-8")).decode("ascii")
            }
        else:
            header = {
                "Content-Type": "application/json",
                "Req-Common-Header":base64.b64encode(ICISCmmn.getServiceRequest(self,prm).encode("utf-8")).decode("ascii")
            }

        return header

    def getHttpExternalheader(self,prm):

        if "rest" in prm.get("endpoint", "").lower():
            header = {
                "commonHeader":ICISCmmn.getCommonHeader(self,prm),
                "bizHeader":ICISCmmn.getBizHeader(self,prm),
                "saSecurity":ICISCmmn.getSaSecurity(self,prm)
            }

        return header

    def getSecurityContext(self, prm):

        securityContext = prm.get("securityContext", "")

        if not (securityContext is not None and
                (isinstance(securityContext, dict) and securityContext) or
                (isinstance(securityContext, str) and securityContext.strip())):

            filter = {
                "bill": {
                    "runAsUser": 703,
                    "runAsGroup": 9999,
                    "fsGroup": 9999,
                    "privileged": False
                },
                "rater": {
                    "runAsUser": 704,
                    "runAsGroup": 9999,
                    "fsGroup": 9999,
                    "privileged": False
                },
                "oder": {
                    "runAsUser": 713,
                    "runAsGroup": 9999,
                    "fsGroup": 9999,
                    "privileged": False
                }
            }
            securityContext = filter.get(self.DOMAIN, {})

        logging.info( f"[CTG:CMMN] getSecurityContext > {securityContext}")
        return securityContext

    def getContainerResources(self, prm):

        resources = prm.get("resources", "")

        if not (resources is not None and
                (isinstance(resources, dict) and resources) or
                (isinstance(resources, str) and resources.strip())):

            filter = {
                "bill": {
                    "requests": {
                        "cpu": "1",
                        "memory": "3Gi"
                    },
                    "limits": {
                        "cpu": "2",
                        "memory": "5Gi"
                    }
                },
                "rater": {
                    "requests": {
                        "memory": "2048Mi"
                    },
                    "limits": {
                        "memory": "2048Mi"
                    }
                },
                "oder": {
                    "requests": {
                        "cpu": "1",
                        "memory": "3Gi"
                    },
                    "limits": {
                        "cpu": "2",
                        "memory": "7Gi"
                    }
                }
            }
            resources = filter.get(self.DOMAIN, {})

        # 최대값 설정
        max_cpu = 8
        max_memory = 20

        for category in ["requests", "limits"]:
            if category in resources:
                if "cpu" in resources[category]:
                    resources[category]["cpu"] = ICISCmmn.limit_value(self, resources[category]["cpu"], max_cpu, "cpu")
                if "memory" in resources[category]:
                    resources[category]["memory"] = ICISCmmn.limit_value(self, resources[category]["memory"], max_memory, "memory")

        logging.info( f"[CTG:CMMN] getContainerResources > {resources}")
        return resources

    def limit_value(slef, value, max_value, resource_type):
        try:
            if resource_type == 'cpu':
                if isinstance(value, str) and value.endswith('m'):
                    return f"{min(int(value[:-1]), max_value * 1000)}m"
                else:
                    float_value = min(float(value), max_value)
                    return f"{float_value:.3f}".rstrip('0').rstrip('.')
            elif resource_type == 'memory':
                if isinstance(value, str):
                    if value.endswith('Gi'):
                        float_value = min(float(value[:-2]), max_value)
                    elif value.endswith('Mi'):
                        float_value = min(float(value[:-2]) / 1024, max_value)
                    else:
                        float_value = min(float(value) / (1024 * 1024 * 1024), max_value)
                else:
                    float_value = min(float(value) / (1024 * 1024 * 1024), max_value)

                integer_part = math.floor(float_value)
                decimal_part = float_value - integer_part
                if decimal_part == 0:
                    return f"{integer_part}Gi"
                else:
                    return f"{float_value:.3f}Gi".rstrip('0').rstrip('.')
        except ValueError:
            # 변환할 수 없는 경우 원래 값을 반환
            return value

    def getICISPipeline(slef, tasks: List[NestedTaskType]) -> List[BaseOperator]:
        logging.info(f"[CTG:CMMN] getICISPipeline > start >  tasks: {tasks}")

        def branch_decision(task_id: str, **context):
            ti = context['ti']
            task_instance = ti.xcom_pull(task_ids=task_id)

            logging.info(f"[CTG:CMMN] Branch decision for {task_id}: result = {task_instance}")
            return f"{task_id}_success" if task_instance else f"{task_id}_failure"

        def create_branch(task_tuple: ICISCmmn.TaskType) -> List[BaseOperator]:
            logging.info(f"[CTG:CMMN] getICISPipeline > create_branch > task_tuple: {task_tuple}")

            if not isinstance(task_tuple, tuple) or len(task_tuple) != 3:
                raise ValueError(f"[CTG:CMMN] Invalid task structure. Expected a tuple of (base, success, failure), got: {task_tuple}")

            main_task, success_task, failure_task = task_tuple

            if not isinstance(main_task, BaseOperator):
                raise TypeError(f"[CTG:CMMN] Base task must be an Airflow BaseOperator, got: {type(main_task)}")

            if success_task is None or failure_task is None:
                raise ValueError("[CTG:CMMN] Both success_task and failure_task must be provided (not None)")

            branch_task = BranchPythonOperator(
                task_id=f'{main_task.task_id}_branch',
                python_callable=branch_decision,
                op_kwargs={'task_id': main_task.task_id},
                provide_context=True,
                trigger_rule=TriggerRule.NONE_SKIPPED
            )

            success_path = EmptyOperator(task_id=f'{main_task.task_id}_success', trigger_rule=TriggerRule.ALL_SUCCESS)
            failure_path = EmptyOperator(task_id=f'{main_task.task_id}_failure', trigger_rule=TriggerRule.ALL_SUCCESS)

            join_task = EmptyOperator(
                task_id=f'{main_task.task_id}_join',
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            )

            main_task >> branch_task >> [success_path, failure_path]

            if isinstance(success_task, tuple):
                success_subtasks = create_branch(success_task)
                success_path >> success_subtasks[0]
                success_subtasks[-1] >> join_task
            elif isinstance(success_task, BaseOperator):
                success_path >> success_task >> join_task
            else:
                raise TypeError(f"[CTG:CMMN] Success task must be a tuple or BaseOperator, got: {type(success_task)}")

            if isinstance(failure_task, tuple):
                failure_subtasks = create_branch(failure_task)
                failure_path >> failure_subtasks[0]
                failure_subtasks[-1] >> join_task
            elif isinstance(failure_task, BaseOperator):
                failure_path >> failure_task >> join_task
            else:
                raise TypeError(f"[CTG:CMMN] Failure task must be a tuple or BaseOperator, got: {type(failure_task)}")

            return [main_task, branch_task, success_path, failure_path, join_task]

        def process_tasks(tasks: List[ICISCmmn.NestedTaskType]) -> List[BaseOperator]:
            processed_tasks = []
            for task in tasks:
                if isinstance(task, tuple):
                    branch_tasks = create_branch(task)
                    processed_tasks.extend(branch_tasks)
                else:
                    processed_tasks.append(task)

            for i in range(len(processed_tasks) - 1):
                current_task = processed_tasks[i]
                next_task = processed_tasks[i + 1]

                if current_task.task_id.endswith('_branch'):
                    continue
                if next_task.task_id.endswith(('_success', '_failure')):
                    continue
                if current_task.task_id.endswith(('_success', '_failure')):
                    if next_task.task_id.endswith('_join'):
                        continue

                current_task >> next_task

            return processed_tasks

        processed_tasks = process_tasks(tasks)
        return processed_tasks

    def getDummyTask(self, task_id: str, state):
        def dummy_task(**kwargs):
            time.sleep(3)
            if state:
                logging.info(f"Task {task_id} succeeded")
                return True
            else:
                logging.info(f"Task {task_id} failed")
                return False

        return PythonOperator(
            task_id=task_id,
            python_callable=dummy_task,
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    ################################ [알람] ################################
    def success_callback(self, wflow_id, task_id):
        ICISCmmn.getICISLog(self, {"task_id": task_id + "_success_callback"})

        url = "http://{}/wflow/alrm/{}/success".format(DEVPILOT_SVC_URL, wflow_id)
        headers = ICISCmmn.getHttpheader(self, {"task_id": wflow_id})

        logging.info(f"[CTG:CMMN] success_callback > Task: [{task_id}]], url: [{url}], headers:[{headers}]")


        response = requests.get(url, headers=headers)

    def failure_callback(self, wflow_id, task_id, context):
        ICISCmmn.getICISLog(self, {"task_id": task_id + "_failure_callback"})

        ti = context["ti"]
        ti.xcom_push(key="task_status", value={"status": "failed"})

        url =  "http://{}/wflow/alrm/{}/fail".format(DEVPILOT_SVC_URL, wflow_id)
        headers = ICISCmmn.getHttpheader(self, {"task_id": wflow_id})

        logging.info(f"[CTG:CMMN] failure_callback > Task: [{task_id}]], url: [{url}], headers: [{headers}]")

        response = requests.get(url, headers=headers)

    def startTaskAlrm(self, prm):
        orgTaskId = prm["task_id"]
        prm["task_id"] = prm.get("task_id", "") + "_startTaskAlrm"
        ICISCmmn.getICISLog(self, prm)
        prm["task_id"] = orgTaskId

        url = "http://{}/wflow/alrm/{}/{}/start".format(DEVPILOT_SVC_URL, self.WORKFLOW_ID, prm["id"])
        headers = ICISCmmn.getHttpheader(self, prm)

        logging.info(f"[CTG:CMMN] startTaskAlrm > Task: [{prm['task_id']}], url: [{url}], headers: [{headers}]")

        response = requests.get(url, headers=headers)

    def endTaskAlrm(self, prm):
        orgTaskId = prm["task_id"]
        prm["task_id"] = prm.get("task_id", "") + "_endTaskAlrm"
        ICISCmmn.getICISLog(self, prm)
        prm["task_id"] = orgTaskId

        url = "http://{}/wflow/alrm/{}/{}/end".format(DEVPILOT_SVC_URL, self.WORKFLOW_ID, prm["id"])
        headers = ICISCmmn.getHttpheader(self, prm)

        logging.info(f"[CTG:CMMN] endTaskAlrm > Task: [{prm['task_id']}], url: [{url}]")

        response = requests.get(url, headers=headers)

    def vrfSuccessAlrm(self, prm):
        orgTaskId = prm["task_id"]
        prm["task_id"] = prm.get("task_id", "") + "_vrfSuccessAlrm"
        ICISCmmn.getICISLog(self, prm)
        prm["task_id"] = orgTaskId

        url = "http://{}/wflow/alrm/{}/{}/verification/success".format(DEVPILOT_SVC_URL, self.WORKFLOW_ID, prm["id"])
        headers = ICISCmmn.getHttpheader(self, prm)

        logging.info(f"[CTG:CMMN] vrfSuccessAlrm > Task: [{prm['task_id']}], url: [{url}]")

        response = requests.get(url, headers=headers)

    def vrfFailureAlrm(self, prm):
        orgTaskId = prm["task_id"]
        prm["task_id"] = prm.get("task_id", "") + "_vrfFailureAlrm"
        ICISCmmn.getICISLog(self, prm)
        prm["task_id"] = orgTaskId

        url = "http://{}/wflow/alrm/{}/{}/verification/fail".format(DEVPILOT_SVC_URL, self.WORKFLOW_ID, prm["id"])
        headers = ICISCmmn.getHttpheader(self, prm)

        logging.info(f"[CTG:CMMN] vrfFailureAlrm > Task: [{prm['task_id']}], url: [{url}]")

        response = requests.get(url, headers=headers)

    ################################ [현재 안쓰는 source / 초기세팅 대상] ################################
    def getUserId(self):
        if self.DOMAIN=="oder":
            return "91337909"
        elif self.DOMAIN=="rater":
            return "91337910"
        elif self.DOMAIN=="bill":
            return "91337930"
        elif self.DOMAIN=="sa":
            return "82258624"
        else:
            return "82258624"
        #fail logic input
        #TODo 도메인별 UserID 리턴
        #ICIS Tr Order  : 91337909
        #ICIS Tr Rater  : 91337910
        #ICIS Tr Bill  : 91337930

    def getChnlType(self):
        if self.DOMAIN=="oder":
            return "TO"
        elif self.DOMAIN=="rater":
            return "TR"
        elif self.DOMAIN=="bill":
            return "TB"
        elif self.DOMAIN=="sa":
            return "UI"
        else:
            return "UI"
        #TODo 도메인별 ChnlType 리턴
        #ICIS Tr Order  : TO
        #ICIS Tr Rater  : TR
        #ICIS Tr Bill  : TB

    def getAppName(self):
        if self.DOMAIN=="oder":
            return "NBSS_TORD"
        elif self.DOMAIN=="rater":
            return "NBSS_TRAT"
        elif self.DOMAIN=="bill":
            return "NBSS_TBIL"
        elif self.DOMAIN=="sa":
            return "NBSS_ICIS"
        else:
            return "NBSS_ICIS"
        #TODo 도메인별 APPNAME 리턴
        #ICIS Tr Order API 연계 시 : NBSS_TORD
        #ICIS Tr Rater API 연계시 : NBSS_TRAT
        #ICIS Tr Bill API 연계 시 : NBSS_TBIL

    # [Class] ############[[2024.04.15 상훈D 요청]] 하위 호환성을 위한 중복 소스 / Iterator3 이전 dag 적용, 4부터는 class 내부함수 적용(devpilot) ####################
    def getICISKubernetesPodOperator(self,prm):
        return ICISCmmn.getICISKubernetesPodOperator_v1(self, prm)

    def getICISSimpleHttpOperator(self,prm):
        return ICISCmmn.getICISSimpleHttpOperator_v1(self, prm)

# [전역] #############################################################################################################################
def getICISAuthCheckWflow(wflow_id):
    if wflow_id is None:
        raise Exception("[CTG:CMMN] ICIS_AuthCheckWflow > Workflow id not found")

    return EmptyOperator(
        task_id = "ICIS_AuthCheckWflow"
    )

def getICISCompleteWflowTask(wflow_id):
    return PythonOperator(task_id="ICIS_CompleteWflow", python_callable = partial(success_callback,wflow_id), trigger_rule = TriggerRule.ALL_SUCCESS)

def success_callback(wflow_id):
    url = "http://{}/wflow/alrm/{}/success".format(DEVPILOT_SVC_URL, wflow_id)
    headers= {"Content-Type": "application/json"}

    logging.info(f"[CTG:CMMN] success_callback > Task: [{wflow_id}]], url: [{url}], headers:[{headers}]")

    response = requests.get(url, headers=headers)
# [Class] ############[[2024.04.15 상훈D 요청]] 하위 호환성을 위한 중복 소스 / Iterator3 이전 dag 적용, 4부터는 class 내부함수 적용(devpilot) ####################
