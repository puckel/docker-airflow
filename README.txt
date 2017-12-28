
1.工作目录
AIRFLOW_HOME_MACH=/data/docker-airflow

2.下载APP
git clone https://github.com/jet2007/docker-airflow

以下进行工作目录 cd $AIRFLOW_HOME_MACH



3 重新buiild
3.1 准备环境
  mysql
    ip 192.168.1.181
    host 3306
    user root
    password 123456
    db airflowmeta (utf8格式建议)
  config/airflow.cfg
    sql_alchemy_conn  : 使用mysql
    celery_result_backend：使用mysql
    flower : host,port,根据实际生产环境，进行修改
    broker_url :redis的host,port,根据实际生产环境，进行修改


3.2 重新buiild puckel/docker-airflow:1.8.2
docker build -t puckel/docker-airflow:1.8.1  . 


