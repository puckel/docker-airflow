# coding:utf-8

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.datax2 import RDMS2RDMSOperator


default_args = {
    'owner': 'luke',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 31),
    'email': ['junping.luo@aqara.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

src_conn_id = "prod_crm_db"
tar_conn_id = "prod_sync_sale_db"


dag = DAG(
    'prod_sync_sale_data', default_args=default_args, schedule_interval=timedelta(minutes=5)
)

task_product_template = RDMS2RDMSOperator(
    task_id="sync_product_template",
    src_conn_id=src_conn_id,
    src_query_sql="select id,name, description_purchase, default_code,create_date,write_date,active from product_template",
    tar_conn_id=tar_conn_id,
    tar_table="product_template",
    tar_columns=["id", "name", "description_purchase", "default_code", "create_date", "write_date", "active"],
    tar_pre_sql="delete from product_template",
    dag=dag,
)

task_product = RDMS2RDMSOperator(
    task_id="sync_product",
    src_conn_id=src_conn_id,
    src_query_sql="SELECT id, weight, default_code, product_tmpl_id, message_last_post, create_uid, write_uid, create_date, barcode, volume, write_date, active FROM  product_product",
    tar_conn_id=tar_conn_id,
    tar_table="product_product",
    tar_columns=["id", "weight", "default_code", "product_tmpl_id", "message_last_post", "create_uid", "write_uid", "create_date", "barcode", "volume", "write_date", "active"],
    tar_pre_sql="delete from product_product",
    dag=dag,
)

task_country = RDMS2RDMSOperator(
    task_id="sync_country_state",
    src_conn_id=src_conn_id,
    src_query_sql="SELECT id, code, name, country_id, create_date, write_date FROM res_country_state",
    tar_conn_id=tar_conn_id,
    tar_table="res_country_state",
    tar_columns=["id", "code", "name", "country_id", "create_date", "write_date"],
    tar_pre_sql="delete from res_country_state",
    dag=dag,
)

task_partner = RDMS2RDMSOperator(
    task_id="sync_partner",
    src_conn_id=src_conn_id,
    src_query_sql="SELECT id, active, street, city, display_name, state_id, create_date, write_date FROM res_partner",
    tar_conn_id=tar_conn_id,
    tar_table="res_partner",
    tar_columns=["id", "active", "street", "city", "display_name", "state_id", "create_date", "write_date"],
    tar_pre_sql="delete from res_partner",
    dag=dag,
)

task_order = RDMS2RDMSOperator(
    task_id="sync_order",
    src_conn_id=src_conn_id,
    src_query_sql="SELECT id, state, amount_total, pay_time, source_status, total_discount, order_id, express_sn, shop_type,shop_create_time,shipping_address_str,order_discount,type,seller_order_discount,is_platform_shipping,origin_total,seller_other_discount,create_date,write_date FROM sale_order",
    tar_conn_id=tar_conn_id,
    tar_table="sale_order",
    tar_columns=["id", "state", "amount_total", "pay_time", "source_status", "total_discount", "order_id", "express_sn", "shop_type", "shop_create_time", "shipping_address_str", "order_discount", "type", "seller_order_discount", "is_platform_shipping", "origin_total", "seller_other_discount", "create_date", "write_date"],
    tar_pre_sql="delete from sale_order",
    dag=dag,
)

task_order_line = RDMS2RDMSOperator(
    task_id="sync_order_line",
    src_conn_id=src_conn_id,
    src_query_sql="SELECT id, price_unit, product_uom_qty, order_id, product_id, name, express_sn, price_subtotal, create_date,write_date FROM sale_order_line",
    tar_conn_id=tar_conn_id,
    tar_table="sale_order_line",
    tar_columns=["id", "price_unit", "product_uom_qty", "order_id", "product_id", "name", "express_sn", "price_subtotal", "create_date", "write_date"],
    tar_pre_sql="delete from sale_order_line",
    dag=dag,
)

task_product_template >> task_product
task_order >> task_order_line
