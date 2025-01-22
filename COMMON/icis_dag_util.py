import json
from datetime import datetime
from typing import List

import pendulum
import requests
import time
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator


class ICISDagUtil:
  """ICIS DAG 유틸리티 클래스"""

  @staticmethod
  def getTask(common: 'ICISCmmn', task_name: str, cluster=None) -> BaseOperator:
    """
    개별 태스크를 생성하는 함수

    Args:
        common: ICISCmmn 인스턴스
        task_name: 태스크 이름

    Returns:
        BaseOperator: 생성된 태스크 오퍼레이터
    """
    argocd_token_cz="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcmdvY2QiLCJzdWIiOiJndWVzdDphcGlLZXkiLCJuYmYiOjE3MzQwNzYyMjUsImlhdCI6MTczNDA3NjIyNSwianRpIjoiM2Y0Yjc0NzItNDJmMi00ZmM2LWExODAtM2RhZjBhYjAzN2IwIn0.oD8I83xGPO0Hz6rKFsk4ZsylVB7r7BEPChAZBdGORys" 
    
    local_tz = pendulum.timezone("Asia/Seoul")
    
    def get_keycloak_token():
      keycloak_host = "https://keycloak.icis.kt.co.kr"
      realm = "icis"
      client_id = "airflow"
      client_secret = "CCf7VWWziJ3y9kwxqgPpSgIWu3rbu2Qm"
      username = "admin"
      password = "new1234!"
      token_url = f"{keycloak_host}/realms/{realm}/protocol/openid-connect/token"

      data = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password
      }
      response = requests.post(token_url, data=data)
      response.raise_for_status()
      return response.json()["access_token"]

    def api_request(method, endpoint, **kwargs):
      AIRFLOW_HOST = "https://airflow.icis.kt.co.kr"
      API_ENDPOINT = f"{AIRFLOW_HOST}/api/v1"
      try:
        token = get_keycloak_token()
        headers = {
          "Authorization": f"Bearer {token}",
          "Content-Type": "application/json"
        }
        response = requests.request(
          method,
          f"{API_ENDPOINT}{endpoint}",
          headers=headers,
          **kwargs
        )
        response.raise_for_status()
        return response.json()
      except Exception as e:
        print(f"API request failed for {endpoint}: {e}")
        return None  # 에러시 None 반환

    def get_filtered_dags():
      offset = 0
      limit = 100
      all_dags = []

      filter_params = {
        'dag_id_pattern': f"icis-{common.DOMAIN}-%.{common.ENV}.%",
      }

      print(f"filter_params: {filter_params}")

      while True:
        params = {
          "limit": limit,
          "offset": offset,
          **filter_params
        }

        response = api_request("GET", "/dags", params=params)
        dags = response.get('dags', [])
        if not dags:
          break

        active_dags = [
          dag['dag_id']
          for dag in dags if (
            not dag['is_paused']
          )
        ]
        all_dags.extend(active_dags)

        offset += limit
        if len(dags) < limit:
          break

      print(f"Total DAGs retrieved: {len(all_dags)}")
      return all_dags

    def pause_active_dag(**context):
      all_dags = get_filtered_dags()

      for dag_id in all_dags:
        print(f"pause_active_dag: {dag_id}")
        # 20260117 아래 주석 제거해야 실행 됨
        time.sleep(0.3)
        api_request("PATCH", f"/dags/{dag_id}", json={"is_paused": True})

      Variable.set(
        f"24x7_{common.DOMAIN}_{common.ENV}_paused_dags",
        json.dumps(all_dags),
        (
          f"24x7 Test"
          f", List of {len(all_dags)} paused DAGs"
          f", Last updated: {datetime.now(local_tz).strftime('%Y-%m-%d %H:%M:%S')}"
        )
      )

      print(f"Paused {len(all_dags)} DAGs. IDs saved for later reactivation.")

    def unpause_active_dag(**context):
      paused_dags = json.loads(Variable.get(f"24x7_{common.DOMAIN}_{common.ENV}_paused_dags", "[]"))
      print(f"unpause_active_dag > paused_dags: {len(paused_dags)}")

      for dag_id in paused_dags:
        print(f"Unpausing DAG: {dag_id}")
        # 20260117 아래 주석 제거해야 실행 됨
        time.sleep(0.3)
        api_request("PATCH", f"/dags/{dag_id}", json={"is_paused": False})

      Variable.set(
        f"24x7_{common.DOMAIN}_{common.ENV}_unpaused_dags",
        json.dumps(paused_dags),
        (
          f"24x7 Test"
          f", List of {len(paused_dags)} unpaused DAGs"
          f", Last updated: {datetime.now(local_tz).strftime('%Y-%m-%d %H:%M:%S')}"
        )
      )

    # 모든 태스크 설정을 정의
    task_configs = {
      # availabilitytest-stop-daemon 시나리오 태스크들
      'stop_daemon': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'stop_daemon',
        'arguments': [f"""oc get rollout -l devpilot/type=daemon --no-headers -n {common.NAMESPACE} | while read rollname a; do oc scale rollout $rollname -n {common.NAMESPACE} --replicas=0; done || echo 'done' """]
      },
      'stop_daemon_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'stop_daemon_lt',
        'arguments': [f"""oc get rollout -l devpilot/type=daemon --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc scale rollout $rollname -n {common.NAMESPACE}-lt --replicas=0; done || echo 'done' """]
      },
      'stop_daemon-test': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'stop_daemon-test',
        'arguments': [f"""oc get po -n {common.NAMESPACE}-app; """]
      },
      # availabilitytest-switch-to-dsstdb 시나리오 태스크들
      'switch_to_drdb': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_drdb',
        'arguments': [f"""oc get configmap -l devpilot/type=online --no-headers -n {common.NAMESPACE} | while read conf a; do oc patch configmap $conf -n {common.NAMESPACE} --type='json' -p='[{{"op": "replace", "path": "/data/DB_URL", "value": "$(oc get configmap $conf -n {common.NAMESPACE} -o=jsonpath={{.data.DB_URL_SCND}})"}}]'; done || echo 'done'"""]
      },
      'switch_to_drdb_name': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_drdb_name',
        'arguments': [f"""oc get configmap -l devpilot/type=online --no-headers -n {common.NAMESPACE} | while read conf a; do oc patch configmap $conf -n {common.NAMESPACE} --type='json' -p='[{{"op": "replace", "path": "/data/ACTIVE_DB", "value": "DB_URL_SCND"}}]'; done || echo 'done'"""]
      },
      'switch_to_drdb_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_drdb_lt',
        'arguments': [f"""oc get configmap -l devpilot/type=online --no-headers -n {common.NAMESPACE}-lt | while read conf _; do oc patch configmap $conf -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "replace", "path": "/data/DB_URL", "value": "$(oc get configmap $conf -n {common.NAMESPACE}-lt -o=jsonpath={{.data.DB_URL_SCND}})"}}]'; done || echo 'done'"""]
      },
      'switch_to_drdb_name_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_drdb_name_lt',
        'arguments': [f"""oc get configmap -l devpilot/type=online --no-headers -n {common.NAMESPACE}-lt | while read conf a; do oc patch configmap $conf -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "replace", "path": "/data/ACTIVE_DB", "value": "DB_URL_SCND"}}]'; done || echo 'done'"""]
      },
      'restart_drdb_pod': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_drdb_pod',
        'arguments': [f"""oc get rollout -l devpilot/type=online --no-headers -n {common.NAMESPACE} | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE} --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}} }}]'; done || echo 'done'"""]
      },
      'restart_drdb_pod_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_drdb_pod_lt',
        'arguments': [f"""oc get rollout -l devpilot/type=online --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}} }}]'; done || echo 'done'"""]
      },
      'restart_drdb_daemon_pod': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_drdb_pod',
        'arguments': [f"""oc get rollout -l devpilot/type=daemon --no-headers -n {common.NAMESPACE} | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE} --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}} }}]'; done || echo 'done'"""]
      },
      'restart_drdb_daemon_pod_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_drdb_pod_lt',
        'arguments': [f"""oc get rollout -l devpilot/type=daemon --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}} }}]'; done || echo 'done'"""]
      },

      # availabilitytest-switch-to-pmdb 시나리오 태스크들
      'switch_to_pmdb': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_pmdb',
        'arguments': [f"""oc get configmap -n {common.NAMESPACE} -o custom-columns=.NAME:.metadata.name,OWN:.data.DB_URL --no-headers | grep -E '.+:.+$' | while read conf _; do oc patch configmap $conf -n {common.NAMESPACE} --type='json' -p='[{{"op": "replace", "path": "/data/DB_URL", "value": "'"$(oc get configmap $conf -n {common.NAMESPACE} -o=jsonpath={{.data.DB_URL_BAK}})"'"}}]'; done || echo 'done'"""]
      },
      'switch_to_pmdb_name': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_pmdb_name',
        'arguments': [f"""oc get configmap -n {common.NAMESPACE} -o custom-columns=.NAME:.metadata.name,OWN:.data.DB_URL --no-headers | grep -E '.+:.+$' | while read conf a; do oc patch configmap $conf -n {common.NAMESPACE} --type='json' -p='[{{"op": "replace", "path": "/data/ACTIVE_DB", "value": "DB_URL_BAK"}}]'; done || echo 'done'"""]
      },
      'switch_to_pmdb_lt': {
        'task_id': 'switch_to_pmdb_lt',
        'arguments': [f"""oc get configmap -n {common.NAMESPACE}-lt -o custom-columns=.NAME:.metadata.name,OWN:.data.DB_URL --no-headers | grep -E '.+:.+$' | while read conf _; do oc patch configmap $conf -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "replace", "path": "/data/DB_URL", "value": "'"$(oc get configmap $conf -n {common.NAMESPACE}-lt -o=jsonpath={{.data.DB_URL_BAK}})"'"}}]'; done || echo 'done'"""]
      },
      'switch_to_pmdb_name_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_pmdb_name_lt',
        'arguments': [f"""oc get configmap -n {common.NAMESPACE}-lt -o custom-columns=.NAME:.metadata.name,OWN:.data.DB_URL --no-headers | grep -E '.+:.+$' | while read conf a; do oc patch configmap $conf -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "replace", "path": "/data/ACTIVE_DB", "value": "DB_URL_BAK"}}]'; done || echo 'done'"""]
      },
      'restart_pmdb_pod': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_pmdb_pod',
        'arguments': [f"""oc get rollout -l devpilot/type=online --no-headers -n {common.NAMESPACE} | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE} --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}}}}]'; done || echo 'done'"""]
      },
      'restart_pmdb_pod_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_pmdb_pod_lt',
        'arguments': [f"""oc get rollout -l devpilot/type=online --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}}}}]'; done || echo 'done'"""]
      },
      'restart_pmdb_daemon_pod': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_drdb_pod',
        'arguments': [f"""oc get rollout -l devpilot/type=daemon --no-headers -n {common.NAMESPACE} | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE} --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}} }}]'; done || echo 'done'"""]
      },
      'restart_pmdb_daemon_pod_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_drdb_pod_lt',
        'arguments': [f"""oc get rollout -l devpilot/type=daemon --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}} }}]'; done || echo 'done'"""]
      },

      # availabilitytest-switch-to-prddb 시나리오 태스크들
      'start_daemon': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'start_daemon',
        'arguments': [f"""oc get rollout -l devpilot/type=daemon --no-headers -n {common.NAMESPACE} | while read rollname a; do oc scale rollout $rollname -n {common.NAMESPACE} --replicas=1; done || echo 'done'"""]
      },
      'start_daemon_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'start_daemon_lt',
        'arguments': [f"""oc get rollout -l devpilot/type=daemon --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc scale rollout $rollname -n {common.NAMESPACE}-lt --replicas=1; done || echo 'done'"""]
      }, 
      'switch_to_prddb': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_prddb',
        'arguments': [f"""oc get configmap -n {common.NAMESPACE} -o custom-columns=.NAME:.metadata.name,OWN:.data.DB_URL_BAK --no-headers | grep -E '.+:.+$' | while read conf a; do oc patch configmap $conf -n {common.NAMESPACE} --type='json' -p='[{{"op": "replace", "path": "/data/DB_URL", "value": "$(oc get configmap $conf -n {common.NAMESPACE} -o=jsonpath={{.data.DB_URL_PRMR}})"}}]'; done || echo 'done'"""]
      },
      'switch_to_prddb_name': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_prddb_name',
        'arguments': [f"""oc get configmap -n {common.NAMESPACE} -o custom-columns=.NAME:.metadata.name,OWN:.data.DB_URL_BAK --no-headers | grep -E '.+:.+$' | while read conf a; do oc patch configmap $conf -n {common.NAMESPACE} --type='json' -p='[{{"op": "replace", "path": "/data/ACTIVE_DB", "value": "DB_URL_PRMR"}}]'; done || echo 'done'"""]
      },
      'switch_to_prddb_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_prddb_lt',
        'arguments': [f"""oc get configmap -n {common.NAMESPACE}-lt -o custom-columns=.NAME:.metadata.name,OWN:.data.DB_URL_BAK --no-headers | while read conf a; do oc patch configmap $conf -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "replace", "path": "/data/DB_URL", "value": "$(oc get configmap $conf -n {common.NAMESPACE}-lt -o=jsonpath={{.data.DB_URL_PRMR}})"}}]'; done || echo 'done'"""]
      },
      'switch_to_prddb_name_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'switch_to_prddb_name_lt',
        'arguments': [f"""oc get configmap -n {common.NAMESPACE}-lt -o custom-columns=.NAME:.metadata.name,OWN:.data.DB_URL_BAK --no-headers | while read conf a; do oc patch configmap $conf -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "replace", "path": "/data/ACTIVE_DB", "value": "DB_URL_PRMR"}}]'; done || echo 'done'"""]
      },
      'restart_prddb_pod': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_prddb_pod',
        'arguments': [f"""oc get rollout --no-headers -n {common.NAMESPACE} | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE} --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}}}}]'; done || echo 'done'"""]
      },
      'restart_prddb_pod_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'restart_prddb_pod_lt',
        'arguments': [f"""oc get rollout --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc patch rollout $rollname -n {common.NAMESPACE}-lt --type='json' -p='[{{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {{"name": "DB_PATCH", "value": "new_value"}}}}]'; done || echo 'done'"""]
      },
      'test_po': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'test_po',
        'arguments': [f"""oc get po"""]
      },
      'test_po_sh': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'test_po',
        'arguments': [f"""cat /script/dr.sh"""]
      },
      'argocd_get_01': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'argocd_get_01',
        'arguments': [f""" curl -X GET  -H 'Content-Type: application/json' -H "Authorization: Bearer {argocd_token_cz}" 'https://argocd.sit.icis.kt.co.kr/api/v1/applications'| jq -r '.items[] | select(.metadata.name | contains("commit-test08")) | .metadata.name' | while read -r app; do echo "applition $app" ; done """]
      },

      #################################################################################################
      # 24X7 스크립트 시작
      'async_gw_close_cz': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'async_gw_close_cz',
        'arguments': [f""" curl -X 'POST' 'https://icis-cmmn-devpilotagent-cz.icis.kt.co.kr/devpilot-agent/trtEgncyApiGw'  -H 'accept: */*' -H 'Content-Type: application/json' -d '{{"env":"prd","appNm":"NBSS_TORD","apiGwNm":"icis-oder-async-apigw","actnType":"emergencyOn"}}' """]
      },
      'async_gw_close_lt_cz': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'async_gw_close_lt_cz',
        'arguments': [f""" curl -X 'POST' 'https://icis-cmmn-devpilotagent-cz.icis.kt.co.kr/devpilot-agent/trtEgncyApiGw'  -H 'accept: */*' -H 'Content-Type: application/json' -d '{{"env":"prd","appNm":"NBSS_TORD_CO","apiGwNm":"icis-oder-async-apigw-lt","actnType":"emergencyOn"}}' """]
      },      
      'async_gw_close_tz': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'async_gw_close_tz',
        'arguments': [f""" curl -X 'POST' 'https://icis-cmmn-devpilotagent-tz.icis.kt.co.kr/devpilot-agent/trtEgncyApiGw'  -H 'accept: */*' -H 'Content-Type: application/json' -d '{{"env":"prd","appNm": "NBSS_TORD","apiGwNm":"icis-oder-async-apigw","actnType":"emergencyOn"}}' """]
      },
      'async_gw_close_lt_tz': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'async_gw_close_lt_tz',
        'arguments': [f""" curl -X 'POST' 'https://icis-cmmn-devpilotagent-tz.icis.kt.co.kr/devpilot-agent/trtEgncyApiGw'  -H 'accept: */*' -H 'Content-Type: application/json' -d '{{"env":"prd","appNm":"NBSS_TORD_CO","apiGwNm":"icis-oder-async-apigw-lt","actnType":"emergencyOn"}}' """]
      },      
      'async_gw_open_cz': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'async_gw_open_cz',
        'arguments': [f""" curl -X 'POST' 'https://icis-cmmn-devpilotagent-cz.icis.kt.co.kr/devpilot-agent/trtEgncyApiGw'  -H 'accept: */*' -H 'Content-Type: application/json' -d '{{"env":"prd","appNm": "NBSS_TORD","apiGwNm":"icis-oder-async-apigw","actnType":"emergencyOff"}}' """]
      }, 
      'async_gw_open_lt_cz': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'async_gw_open_lt_cz',
        'arguments': [f""" curl -X 'POST' 'https://icis-cmmn-devpilotagent-cz.icis.kt.co.kr/devpilot-agent/trtEgncyApiGw'  -H 'accept: */*' -H 'Content-Type: application/json' -d '{{"env":"prd","appNm":"NBSS_TORD_CO","apiGwNm":"icis-oder-async-apigw-lt","actnType":"emergencyOff"}}' """]
      }, 
      'async_gw_open_tz': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'async_gw_open_tz',
        'arguments': [f""" curl -X 'POST' 'https://icis-cmmn-devpilotagent-tz.icis.kt.co.kr/devpilot-agent/trtEgncyApiGw'  -H 'accept: */*' -H 'Content-Type: application/json' -d '{{"env":"prd","appNm": "NBSS_TORD","apiGwNm":"icis-oder-async-apigw","actnType":"emergencyOff"}}' """]
      },
      'async_gw_open_lt_tz': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'async_gw_open_lt_tz',
        'arguments': [f""" curl -X 'POST' 'https://icis-cmmn-devpilotagent-tz.icis.kt.co.kr/devpilot-agent/trtEgncyApiGw'  -H 'accept: */*' -H 'Content-Type: application/json' -d '{{"env":"prd","appNm":"NBSS_TORD_CO","apiGwNm":"icis-oder-async-apigw-lt","actnType":"emergencyOff"}}' """]
      },
      'stop_consumer': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'stop_consumer',
        'arguments': [f"""oc get rollout -l devpilot/type=consumer --no-headers -n {common.NAMESPACE} | while read rollname a; do oc scale rollout $rollname -n {common.NAMESPACE} --replicas=0; done || echo 'done' """]
      },
      'stop_consumer_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'stop_consumer_lt',
        'arguments': [f"""oc get rollout -l devpilot/type=consumer --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc scale rollout $rollname -n {common.NAMESPACE}-lt --replicas=0; done || echo 'done' """]
      },   
      'stop_batchagent': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'stop_batchagent',
        'arguments': [f"""oc get deployment -l devpilot/type=batchagent --no-headers -n {common.NAMESPACE} | while read rollname a; do oc scale deployment $rollname -n {common.NAMESPACE} --replicas=0; done || echo 'done' """]
      },                                    
      'stop_batchagent_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'stop_batchagent_lt',
        'arguments': [f"""oc get deployment -l devpilot/type=batchagent --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc scale deployment $rollname -n {common.NAMESPACE}-lt --replicas=0; done || echo 'done' """]
      },
      'start_consumer': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'start_consumer',
        'arguments': [f"""oc get rollout -l devpilot/type=consumer --no-headers -n {common.NAMESPACE} | while read rollname a; do oc scale rollout $rollname -n {common.NAMESPACE} --replicas=3; done || echo 'done'"""]
      },
      'start_consumer_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'start_consumer_lt',
        'arguments': [f"""oc get rollout -l devpilot/type=consumer --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc scale rollout $rollname -n {common.NAMESPACE}-lt --replicas=3; done || echo 'done'"""]
      }, 
      'start_batchagent': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'start_batchagent',
        'arguments': [f"""oc get deployment -l devpilot/type=batchagent --no-headers -n {common.NAMESPACE} | while read rollname a; do oc scale deployment $rollname -n {common.NAMESPACE} --replicas=1; done || echo 'done'"""]
      },
      'start_batchagent_lt': {
        'operator_type': 'KubernetesPodOperator',
        'task_id': 'start_batchagent_lt',
        'arguments': [f"""oc get deployment -l devpilot/type=batchagent --no-headers -n {common.NAMESPACE}-lt | while read rollname a; do oc scale deployment $rollname -n {common.NAMESPACE}-lt --replicas=1; done || echo 'done'"""]
      },
      # 24X7 스크립트 종료
      #################################################################################################
      # Python Operator 태스크 추가
      'pause_active_dag': {
        'operator_type': 'PythonOperator',
        'task_id': 'pause_active_dag',
        'python_callable': pause_active_dag
      },
      'unpause_active_dag': {
        'operator_type': 'PythonOperator',
        'task_id': 'unpause_active_dag',
        'python_callable': unpause_active_dag
      }
    }

    if task_name not in task_configs:
      raise ValueError(f"Unknown task: {task_name}")

    task_config = task_configs[task_name]

    # Python Operator인 경우
    if task_config.get('operator_type') == 'PythonOperator':
      return PythonOperator(
        task_id=task_config['task_id'],
        python_callable=task_config['python_callable'],
        provide_context=True
      )

    suffix = '_cz' if (cluster is None or cluster.lower() == "cz") else '_tz'
    task_config['task_id'] = f"{task_config['task_id']}{suffix}"

    # Kubernetes Pod Operator인 경우 (기존 로직)

    task_config.update({
      'volumes': [],
      'volume_mounts': [],
      'env_from': [],
      'image': '/icis/origin-cli:1.0.2',
      'cmds': "Done"
    })
    return common.getICISKubernetesPodOperator_v1(task_config, cluster)

  @staticmethod
  def getScenario(common: 'ICISCmmn', scenario: str, cluster=None) -> List[BaseOperator]:
    """
    시나리오별 태스크들을 생성하는 함수

    Args:
        common: ICISCmmn 인스턴스
        scenario: 시나리오 이름

    Returns:
        List[BaseOperator]: 생성된 태스크 오퍼레이터들의 리스트
    """
    scenario_configs = {
      'availability-switch-to-24by7db': [
        'stop_daemon',
        'stop_daemon_lt'
        'switch_to_pmdb',
        'switch_to_pmdb_name',
        'switch_to_pmdb_lt',
        'switch_to_pmdb_name_lt',
        'restart_pmdb_pod',
        'restart_pmdb_pod_lt'
      ],
      'availability-switch-to-prddb': [
        'start_daemon',
        'start_daemon_lt',
        'switch_to_prddb',
        'switch_to_prddb_name',
        'switch_to_prddb_lt',
        'switch_to_prddb_name_lt',
        'restart_prddb_pod',
        'restart_prddb_pod_lt'
      ],
      'availability-switch-to-drtdb': [
        'switch_to_drdb',
        'switch_to_drdb_name',
        'switch_to_drdb_lt',
        'switch_to_drdb_name_lt',
        'restart_drdb_pod',
        'restart_drdb_pod_lt'
      ],
      'availability-switch-to-24by7db-test': [
        'test_po'
      ],
      'availability-switch-to-prddb-test': [
        'test_po'
      ],
      'availability-switch-to-drtdb-test': [
        'test_po'
      ],
      'availabilitytest-suspend-batch-pause': [
        'pause_active_dag'
      ],
      'availabilitytest-suspend-batch-unpause': [
        'unpause_active_dag'
      ],
      'availabilitytest-stop-daemon-test': [
        'stop_daemon-test'
      ],
      'availabilitytest-test': [
        'test_po'
      ],
      'availabilitytest-test-sh': [
        'test_po_sh'
      ],
      'availabilitytest-argo': [
        'argocd_get_01'
      ]
    }

    if scenario not in scenario_configs:
      raise ValueError(f"Unknown scenario: {scenario}")

    tasks = [ICISDagUtil.getTask(common, task_name, cluster) for task_name in scenario_configs[scenario]]

    # 순차 실행을 위한 의존성 설정
    for i in range(len(tasks)-1):
      tasks[i] >> tasks[i+1]

    return tasks
