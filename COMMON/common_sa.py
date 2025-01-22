class CommonSa:

  @classmethod
  def getEnv(cls, dag_name, task_id):
    env_settings = {
      # WORKFLOW_NAME
      "sytest1": {
        # WORKFLOW_NAME에 속한 TASK_NAME
        "pod": {
          # jvm 설정
          "jvm": (
            "-XX:+UseContainerSupport"
            " -XX:InitialRAMPercentage=50.0"
            " -XX:MaxRAMPercentage=75.0"
            " -XX:+UseG1GC"
            " -XX:MaxGCPauseMillis=200"
            " -XX:+ParallelRefProcEnabled"
            # jvm heap dump 설정 추가
            # /app/sa 경로 확인
            # /heapdump/${NODE_NAME}_${SYS_DATE}_${POD_NAME}_heapdump.hprof는 고정
            " -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/sa/heapdump/${NODE_NAME}_${SYS_DATE}_${POD_NAME}_heapdump.hprof"
          ),
          # resoutces 설정
          "resources": {
            "requests": {
              "cpu": "1",
              "memory": "2Gi"
            },
            "limits": {
              "cpu": "2",
              "memory": "4Gi"
            }
          },
          # security_context 설정
          "security_context": {}
        },
        # WORKFLOW_NAME에 속한 TASK_NAME
        "pod2": {
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
      },
      # WORKFLOW_NAME
      "satest2": {
        # WORKFLOW_NAME에 속한 TASK_NAME
        "pod": {
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