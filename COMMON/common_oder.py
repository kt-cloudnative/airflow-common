class CommonOder:

  @classmethod
  def getEnv(cls, dag_name, task_id):
    env_settings = {
      "SAMPLE_WORKFLOW_NAME_1": {
        "SAMPLE_TASK_NAME_1": {
          "jvm": (
            "-XX:+UseContainerSupport"
            " -XX:InitialRAMPercentage=50.0"
            " -XX:MaxRAMPercentage=75.0"
            " -XX:+UseG1GC"
            " -XX:MaxGCPauseMillis=200"
            " -XX:+ParallelRefProcEnabled"
            # jvm heap dump 설정 추가
            # /app/order 경로 확인
            # /heapdump/${NODE_NAME}_${SYS_DATE}_${POD_NAME}_heapdump.hprof는 고정
            " -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/oder/heapdump/${NODE_NAME}_${SYS_DATE}_${POD_NAME}_heapdump.hprof"
          ),
          "resources": {
            "requests": {
              "cpu": "1",
              "memory": "3Gi"
            },
            "limits": {
              "cpu": "3",
              "memory": "7Gi"
            }
          },
          "security_context": {
            "runAsUser": 713,
            "runAsGroup": 9999,
            "fsGroup": 9999,
            "privileged": False
          }
        },
        "SAMPLE_TASK_NAME_2": {
          "jvm": "-Xms512m -Xmx4G",
          "resources": {
            "requests": {
              "cpu": "2",
              "memory": "4Gi"
            },
            "limits": {
              "cpu": "4",
              "memory": "8Gi"
            }
          },
          "security_context": {
            "runAsUser": 713,
            "runAsGroup": 9999,
            "fsGroup": 9999,
            "privileged": False
          }
        }
      }
    }

    return env_settings.get(dag_name, {}).get(task_id, {})