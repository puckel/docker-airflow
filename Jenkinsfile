pipeline {
    agent any
    options {
        timestamps()
    }
    environment {
        BUILD_STRING="${env.BUILD_NUMBER}-${env.GIT_COMMIT}"
        PROD_IMAGE="347708466071.dkr.ecr.us-east-1.amazonaws.com/classdojo/airflow:$BUILD_STRING"
    }

    stages {
        stage('Docker Build') {
            steps {
                sh'''#!/bin/bash
                    docker build . -f Dockerfile \
                        --build-arg AIRFLOW_DEPS="datadog,statsd" \
                        --build-arg AIRFLOW_CONFIG="$(consul kv get -http-addr=consul.internal.classdojo.com config/airflow)" \
                        -t $PROD_IMAGE

                    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 347708466071.dkr.ecr.us-east-1.amazonaws.com
                    docker push $PROD_IMAGE

                    echo "Latest image available at: $PROD_IMAGE"

                    '''
            }
        }

        stage ('Nomad Plan'){
            steps {
                sh '''
                # --------------------------------- #
                set +e
                <<'COMMENT'
                Plan will return one of the following exit codes:
                * 0: No allocations created or destroyed.
                * 1: Allocations created or destroyed.
                * 255: Error determining plan results.'
                COMMENT

                nomad plan -address="https://nomad.internal.classdojo.com" airflow.nomad
                set -e
                PLAN_RET_CODE=$?
                if [[ "${PLAN_RET_CODE}" == "255" ]]; then
                    echo "Nomad Plan: 255: Error determining plan results"
                    exit ${PLAN_RET_CODE}
                fi
                '''
            }
        }

        stage ('Deploy') {
            when {
                anyOf {
                    branch 'master';
                }
            }
            options { lock('deploy-airflow-lock') }
            steps {
                script {
                    def levant_docker = docker.image('347708466071.dkr.ecr.us-east-1.amazonaws.com/classdojo/levant:latest')
                    levant_docker.pull()
                    levant_docker.inside {
                        sh """#!/bin/sh
                            levant render -var 'DOCKER_IMAGE_ID=${PROD_IMAGE}' \
                                -consul-address http://consul.internal.classdojo.com \
                                -out "airflow.nomad" "airflow.nomad";

                            levant deploy \
                            -address=https://nomad.internal.classdojo.com \
                            airflow.nomad
                            """

                    }

                }
            }
        }
    }
    post {
        always {
            script {
                if (env.BRANCH_NAME == 'master') {
                    notifySlack(currentBuild.result, "#team-blue-bots", "Pipeline Complete", "Airflow Pipeline finished\n")
                }
            }
        }
        cleanup{
            dir("${env.WORKSPACE}@tmp") {
                deleteDir()
            }
            dir("${env.WORKSPACE}@script") {
                deleteDir()
            }
            dir("${env.WORKSPACE}@script@tmp") {
                deleteDir()
            }
            deleteDir()
        }
    }

}
