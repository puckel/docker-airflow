job "airflow" {
  datacenters = ["dc1"]
  region      = "global"
  type        = "service"

  update {
    stagger      = "10s"
    max_parallel = 1
  }

  meta {
    VERSION        = "latest"
    UNIQUE_META_ID = 5
  }

  constraint {
    attribute = "${node.class}"
    value     = "airflow"
  }

  group "airflow" {
    count = 1

    meta {
      "replicator_enabled"          = false
      "replicator_cooldown"         = "20s"
      "replicator_max"              = 5
      "replicator_min"              = 1
      "replicator_scalein_mem"      = 65
      "replicator_scalein_cpu"      = 65
      "replicator_scaleout_mem"     = 70
      "replicator_scaleout_cpu"     = 70
      "replicator_notification_uid" = "airflow"
    }

    task "airflow" {
      driver = "docker"

      env {
        AIRFLOW_DEPS = "datadog"
        LOAD_EX      = "y"
        EXECUTOR     = "Local"
        POSTGRES_HOST = "airflow.cieeljuzwsdg.us-east-1.rds.amazonaws.com"
        POSTGRES_PORT = "5432"
      }

      user = "0"

      config {
        image      = "[[ .DOCKER_IMAGE_ID ]]"
        force_pull = true

        port_map {
          http = 8080
        }

        logging {
          type = "json-file"
        }

        # These labels need the dd-agent docker.d/conf.yaml to be set which isn't on our current ami
        # Enable when new AMI has it.
        labels {
          com.datadoghq.ad.check_names  = "[\"process\"]"
          com.datadoghq.ad.init_configs = "[{\"pid_cache_duration\": \"30\"}]"
          com.datadoghq.ad.instances    = "[{\"host\": \"%%host%%\", \"port\": \"%%port%%\", \"airflow\"}]"
          com.datadoghq.ad.logs         = "[{\"source\": \"python\", \"service\": \"airflow\"}]"
        }
      }

      resources {
        cpu    = 10200 # 3 cores
        memory = 8192  # 8 GB

        network {
          port "http" {
            static = "9001"
          }
        }
      }

      template {
        data        = "{{ key \"config/airflow\" }}"
        destination = "local/airflow/airflow.cfg"
        change_mode = "restart"
      }

      service {
        name = "airflow"

        tags = [
          "airflow",
          "prod",
          "{ \"haproxy_check_tcp\": ${NOMAD_HOST_PORT_http} }",
        ]

        port = "http"

        check {
          name     = "alive"
          type     = "tcp"
          interval = "10s"
          timeout  = "2s"
        }
      }
    }
  }
}
