from __future__ import annotations

import random
import sys
from datetime import datetime, timedelta
from functools import partial

from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.timetables.interval import DeltaDataIntervalTimetable
from airflow.timetables.trigger import CronTriggerTimetable
from kubernetes.client import models as k8s

sys.path.append('/opt/bitnami/airflow/dags/git_sa-common') #common repo dir in cluster
import kubernetes_pod
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import json

import requests
import common_header_staging
import biz_header_staging
import icis_log_dto_staging
import uuid

NEXUS_URL='nexus.dspace.kt.co.kr/'
DEVPILOT_SVC_URL = 'icis-sa-devpilot-backend.devpilot.svc/api/v1'
TIME_ZONE = pendulum.timezone("Asia/Seoul")

def choose_branch(**kwargs):
    branches = ['authFail', 'authSuccess']
    randVal=random.choice(branches)
    isTrue = True if randVal == 'authSuccess' else False
    isTrue = True
    if isTrue:
        task_id = kwargs['next_task_id']
    else:
        task_id = 'authCheckfail'
        #fail logic input
    return task_id

def auth_check(response):
    try:
        isAuth = response.json().get('data',False)
        if isAuth is True:
            print('This workflow is authorized')
            return True
        else:
            raise Exception("This workflow is not authorized")
    except ValueError:
        raise Exception("This workflow is not authorized")

def getICISAuthCheckTask(next_task_id):
    return BranchPythonOperator(
        task_id='ICIS_AuthCheck',
        python_callable=choose_branch,
        op_kwargs={ 'next_task_id':next_task_id }
    )

def getICISAuthCheckWflow(wflow_id):
    if wflow_id is None:
        raise Exception("Workflow id not found")

    url = "{}/wflow/test/{}/auth".format(DEVPILOT_SVC_URL, wflow_id)

    return SimpleHttpOperator(
        headers={"Content-Type": "application/json"},
        task_id='ICIS_AuthCheckWflow',
        method='GET',
        http_conn_id='kubernetes_default',
        endpoint=url,
        log_response=True,
        response_check = auth_check
    )

def success_callback(wflow_id):
    url = "http://{}/wflow/alrm/{}/success".format(DEVPILOT_SVC_URL, wflow_id)
    headers= {"Content-Type": "application/json"}
    response = requests.get(url, headers=headers)

def getICISCompleteWflowTask(wflow_id):
    return PythonOperator(task_id='completeWflow', python_callable = partial(success_callback,wflow_id), trigger_rule = TriggerRule.ALL_SUCCESS)

def getICISCompleteTask():
    return BashOperator(task_id='complete', bash_command='echo Flow 완료')

def getICISConfigMap(configmap_name):
    return k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap_name))

def getICISSecret(secret_name):
    return k8s.V1EnvFromSource(secret_ref= k8s.V1SecretEnvSource(name=secret_name))

def getVolume(volume_name,pvc_name):
    return k8s.V1Volume(name=volume_name, persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name))

def getVolumeMount(volume_name,m_path):
    return k8s.V1VolumeMount(name=volume_name, mount_path=m_path, sub_path=None, read_only=False)

def getAllVolumes(prm):
    volumes = []
    if "volumes" in prm:
        for list in prm['volumes']:
            volumes.append(list)
    return volumes

def getAllVolumeMount(prm):
    volume_mounts = []
    if "volume_mounts" in prm:
        for list in prm['volume_mounts']:
            volume_mounts.append(list)
    return volume_mounts

def getICISVolumeJks():
    return k8s.V1Volume(name='truststore-jks', config_map=k8s.V1ConfigMapVolumeSource(name='truststore.jks'))

def getICISVolumeMountJks():
    return k8s.V1VolumeMount(name='truststore-jks', mount_path='/app/resources/truststore.jks', sub_path='truststore.jks', read_only=False)

def getICISHostAliases():
    return k8s.V1HostAlias(ip='10.217.137.12',hostnames=['sa-cluster-kafka-0.sit.icis.kt.co.kr','sa-cluster-kafka-1.sit.icis.kt.co.kr','sa-cluster-kafka-2.sit.icis.kt.co.kr'])

#오류처리필요
def getAccess_control(DOMAIN):
    return {
            "icis-"+DOMAIN+"_user":{"can_dag_read"},
            "icis-"+DOMAIN+"_admin":{"can_dag_read","can_dag_edit"}
            }
def ICISCommonEnvVars():
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
                }
            ]

def customSchedule(schedule_interval, timezone=TIME_ZONE):
    if schedule_interval == '@once':
        return {'schedule': '@once'}

    if isinstance(schedule_interval, str):
        if schedule_interval.startswith('@'):
            return {'timetable': getPresetTimetable(schedule_interval, timezone)}
        else:
            return {'timetable': CronTriggerTimetable(schedule_interval, timezone=timezone)}
    elif isinstance(schedule_interval, timedelta):
        return {'timetable': DeltaDataIntervalTimetable(schedule_interval, timezone=timezone)}
    else:
        raise ValueError(f"Unsupported schedule type: {type(schedule_interval)}")

def getPresetTimetable(preset: str, timezone):
    presets = {
        '@daily': DeltaDataIntervalTimetable(timedelta(days=1), timezone=timezone),
        '@hourly': DeltaDataIntervalTimetable(timedelta(hours=1), timezone=timezone),
        '@weekly': DeltaDataIntervalTimetable(timedelta(weeks=1), timezone=timezone),
        '@monthly': CronTriggerTimetable("0 0 1 * *", timezone=timezone),
        '@yearly': CronTriggerTimetable("0 0 1 1 *", timezone=timezone),
    }
    if preset in presets:
        return presets[preset]
    raise ValueError(f"Unsupported schedule preset: {preset}")

class ICISCmmn():
    DOMAIN=''
    ENV=''

    NAMESPACE=''
    IMG_INFO_JSON_FILE_PATH=''#img tag

    def __init__(self, DOMAIN, ENV, NAMESPACE):
        self.ENV = ENV
        self.DOMAIN = DOMAIN
        self.NAMESPACE = NAMESPACE


    def getICISDAG(self,prm):
        return DAG(
            access_control=getAccess_control(self.DOMAIN),
            default_args={
                'owner': self.DOMAIN,
                'depends_on_past': False,
                #'email': ['cjs@gmail.com'],#추후 개발 사항
                'retriey': 1,
                'retry_delay': timedelta(minutes=5)
            },
            tags=[self.DOMAIN,self.ENV],
            catchup=False,#backfill
            dag_id=prm['dag_id'], #airflow에 보여질 dag_id 중복불가
            # schedule_interval=prm['schedule_interval'], # scheduling
            **customSchedule(prm["schedule_interval"]),
            start_date=prm['start_date'],
            end_date=prm.get('end_date', None), # scheduling TO DO 수정필요
            # is_paused_upon_creation=prm.get('paused', False)
            is_paused_upon_creation=True
        )

    def failure_callback(self, task_id, context):
        ti = context['ti']
        ti.xcom_push(key='task_status', value={'status': 'failed'})

        url =  "http://{}/wflow/alrm/{}/fail".format(DEVPILOT_SVC_URL, task_id)
        headers= {"Content-Type": "application/json"}
        response = requests.get(url, headers=headers)

    def strToBool(self):
        if self in ["TRUE","True","true", "1"]:
            return True
        else:
            return False

    def getICISKubernetesPodOperator(self,prm):
        repoaddr = Variable.get("repoaddr", "")
        cluster = Variable.get("cluster", "")
        context = Variable.get("context", "")
        config = Variable.get("config", "")
        path = prm["image"]
        image = f"{repoaddr}{path}"

        return kubernetes_pod.KubernetesPodOperator(
            in_cluster=ICISCmmn.strToBool(cluster),
            cluster_context=context,
            config_file=config,
            volumes=getAllVolumes(prm),
            volume_mounts=getAllVolumeMount(prm),
            env_from=prm['env_from'], #환경별 정보 (configmap,secret)
            task_id=prm['task_id'],
            image=prm['image'],
            arguments=prm['arguments'],
            namespace=self.NAMESPACE,
            get_logs=True,
            image_pull_secrets = [k8s.V1LocalObjectReference('dspace-nexus')],
            name=prm['task_id'],
            is_delete_operator_pod = True,
            env_vars=ICISCommonEnvVars(),
            host_aliases=[getICISHostAliases()],
            on_failure_callback = partial(self.failure_callback, prm.get('id','')),
            trigger_rule= prm.get('trigger_rule', TriggerRule.ALL_SUCCESS)
        )

    def getGlobalNo(user_id):
        return (user_id+""+datetime.today().strftime('%Y%m%d%H%M%S')+""+str(random.randint(100000,999999))).zfill(32)

    def mkHeader(prm):
        if "data" in prm:
            header = {
                 "service_request": {
                     "payload": prm['data'],
                     "commonHeader": ICISCmmn.getCommonHeader(),
                     "bizHeader": ICISCmmn.getBizHeader()
                 }
             }
            return json.dumps(header)
        else:
            header = {
            "service_request": {
                "commonHeader": ICISCmmn.getCommonHeader(),
                "bizHeader": ICISCmmn.getBizHeader()
                }
            }
            return json.dumps(header)


    def getBizHeader():
        return biz_header_staging.BizHeader(
            orderId="",
            cbSvcName="/ppon/syscd/retvByGrpId",
            cbFnName="service"
        ).__dict__

    def getCommonHeader():
        return common_header_staging.CommonHeader(
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
            globalNo=ICISCmmn.getGlobalNo("82258624"),
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
        ).__dict__

    def getICISLog():
        print(json.dumps(icis_log_dto_staging.ICISLog(
            SERVICE="AIRFLOW",
            TYPE="BATCH",
            TRACE = str(uuid.uuid1())+":0",
            CATEGORY="DEBUG",
            DATE=datetime.today().strftime('%Y%m%d%H%M%S'),
            SOURCE=".py or task_id",
            LOGLEVEL="INFO",
            HEADER= "str", #공통헤더,
            MESSAGE= "MESSAGE" #내용
        ).__dict__))

    def getICISSimpleHttpOperator(self,prm):
        ICISCmmn.getICISLog()
        return SimpleHttpOperator(
            headers= prm.get('headers', {}),
            task_id=prm['task_id'],
            method=prm['method'] if "method" in prm else 'GET',
            http_conn_id='kubernetes_default',
            data= json.dumps(prm.get('data',{})),
            endpoint=prm['endpoint'],
            log_response=True,
            response_filter= lambda res: json.loads(res.text),
            on_failure_callback = partial(self.failure_callback,  prm.get('id','')),
            trigger_rule= prm['trigger_rule'] if "trigger_rule" in prm else TriggerRule.ALL_SUCCESS
        )

    def getICISEmptyOperator(self,prm):
        return EmptyOperator(task_id=prm["task_id"])

