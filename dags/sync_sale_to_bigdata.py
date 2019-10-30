# coding:utf-8

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.datax import RDMS2RDMSOperator


default_args = {
    'owner': 'luke',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 25),
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


dag = DAG(
    'sync_sale_data', default_args=default_args, schedule_interval=timedelta(days=1)
)

task1 = RDMS2RDMSOperator(
    task_id="sync_sale_order",
    src_conn_id="test_sale_db",
    src_query_sql="SELECT o.id, o.name, o.date_order, o.commitment_date, o.state , o.amount_untaxed, o.amount_tax, o.lumi_total_discount, o.amount_express, o.lumi_origin_price_total , o.amount_total, p.name AS partner_name, cc.name AS customer_category, pp.name AS pricelist, apt.name AS payment_term , ct.name AS team, u.name AS salesman, company.name AS company FROM sale_order o LEFT JOIN res_partner p ON o.partner_id = p.id LEFT JOIN lumi_sale_customer_category cc ON o.customer_category_id = cc.id LEFT JOIN product_pricelist pp ON o.pricelist_id = pp.id LEFT JOIN ( SELECT account_payment_term.id, ir_translation.value AS name FROM account_payment_term LEFT JOIN ir_translation ON account_payment_term.id = ir_translation.res_id WHERE ir_translation.name = 'account.payment.term,name' AND ir_translation.lang = 'zh_CN' ) apt ON o.payment_term_id = apt.id LEFT JOIN ( SELECT crm_team.id, ir_translation.value AS name FROM crm_team LEFT JOIN ir_translation ON crm_team.id = ir_translation.res_id WHERE ir_translation.name = 'crm.team,name' AND ir_translation.lang = 'zh_CN' ) ct ON ct.id = o.team_id LEFT JOIN ( SELECT res_users.id, res_partner.name FROM res_users LEFT JOIN res_partner ON res_users.partner_id = res_partner.id ) u ON u.id = o.user_id LEFT JOIN res_company company ON company.id = o.company_id",
    tar_conn_id="local_pg_master",
    tar_table="sale_order",
    tar_columns=["id", "name", "date_order", "commitment_date","state", "amount_untaxed", "amount_tax", "lumi_total_discount", "amount_express", "lumi_origin_price_total", "amount_total","partner_name", "customer_category", "pricelist", "payment_term", "team", "salesman", "company"],
    tar_pre_sql="delete from sale_order",
    dag=dag,
)
