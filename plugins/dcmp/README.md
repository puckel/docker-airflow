# Airflow DAG Creation Manager Plugin

### Description
A plugin for [Apache Airflow](https://github.com/apache/incubator-airflow) that create and manage your DAG with web UI.

The plugin also provide other custom features for your DAG.

Caution:  You can only list and edit your DAG created by DAG Creation Manager.

### System Requirements

* Airflow Versions
    * 1.X

### Using DAG Creation Manager

Once you deploy the plugin and restart the web server, you can start to use the DAG Creation Manager. When you reload the Airflow Web Server page, you will see a link under the Admin tab called "DAG Creation Manager". Clicking on the link will navigate you to the following URL:

http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/dagcreationmanager/list

Now, you can create and manage your DAG in this web page.