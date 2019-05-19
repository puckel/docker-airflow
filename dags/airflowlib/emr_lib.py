import json,  time, logging, requests
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.hooks.aws_hook import AwsHook
from datetime import datetime


def client(clientID):
    global emr
    emr = EmrHook.get_connection(clientID)

def get_security_group_id(group_name, region_name):
    awsConn = AwsHook.get_connection("AWSConnecionID")
    ec2 = awsConn.client("ec2")
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']

def create_cluster(region_name, cluster_name='Airflow-' + str(datetime.now()), release_label='emr-5.9.0',master_instance_type='m3.xlarge', num_core_nodes=2, core_node_instance_type='m3.2xlarge'):
    emr_master_security_group_id = get_security_group_id('AirflowEMRMasterSG', region_name=region_name)
    emr_slave_security_group_id = get_security_group_id('AirflowEMRSlaveSG', region_name=region_name)
    cluster_response = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': core_node_instance_type,
                    'InstanceCount': num_core_nodes
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'airflow_key_pair',
            'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },
        VisibleToAllUsers=True,
        JobFlowRole='EmrEc2InstanceProfile',
        ServiceRole='EmrRole',
        Applications=[
            { 'Name': 'hadoop' },
            { 'Name': 'spark' },
            { 'Name': 'hive' },
            { 'Name': 'livy' },
            { 'Name': 'zeppelin' }
        ]
    )
    return cluster_response['JobFlowId']


def get_cluster_dns(cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']


def wait_for_cluster_creation(cluster_id):
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)


def terminate_cluster(cluster_id):
    emr.terminate_job_flows(JobFlowIds=[cluster_id])

# Creates an interactive scala spark session.
# Python(kind=pyspark), R(kind=sparkr) and SQL(kind=sql) spark sessions can also be created by changing the value of kind.
def create_spark_session(master_dns, kind='spark'):
    # 8998 is the port on which the Livy server runs
    host = 'http://' + master_dns + ':8998'
    data = {'kind': kind}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    logging.info(response.json())
    return response.headers


def wait_for_idle_session(master_dns, response_headers):
    # wait for the session to be idle or ready for job submission
    status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location']
    while status != 'idle':
        time.sleep(3)
        status_response = requests.get(session_url, headers=response_headers)
        status = status_response.json()['state']
        logging.info('Session status: ' + status)
    return session_url


def kill_spark_session(session_url):
    requests.delete(session_url, headers={'Content-Type': 'application/json'})

# Submits the scala code as a simple JSON command to the Livy server
def submit_statement(session_url, statement_path):
    statements_url = session_url + '/statements'
    with open(statement_path, 'r') as f:
        code = f.read()
    data = {'code': code}
    response = requests.post(statements_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    logging.info(response.json())
    return response

# Function to help track the progress of the scala code submitted to Apache Livy
def track_statement_progress(master_dns, response_headers):
    statement_status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location'].split('/statements', 1)[0]
    # Poll the status of the submitted scala code
    while statement_status != 'available':
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
        statement_url = host + response_headers['location']
        statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
        statement_status = statement_response.json()['state']
        logging.info('Statement status: ' + statement_status)

        #logging the logs
        lines = requests.get(session_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
            logging.info(line)

        if 'progress' in statement_response.json():
            logging.info('Progress: ' + str(statement_response.json()['progress']))
        time.sleep(10)
    final_statement_status = statement_response.json()['output']['status']
    if final_statement_status == 'error':
        logging.info('Statement exception: ' + statement_response.json()['output']['evalue'])
        for trace in statement_response.json()['output']['traceback']:
            logging.info(trace)
        raise ValueError('Final Statement Status: ' + final_statement_status)
    logging.info('Final Statement Status: ' + final_statement_status)


def get_public_ip(cluster_id):
    instances = emr.list_instances(ClusterId=cluster_id, InstanceGroupTypes=['MASTER'])
    return instances['Instances'][0]['PublicIpAddress']
