#!/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
import logging
import os
import pprint
import re
import time
from datetime import timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import DagRun, TaskInstance, settings
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (BranchPythonOperator,
                                               PythonOperator,
                                               ShortCircuitOperator)
from airflow.utils.dates import cron_presets
from airflow.utils.helpers import chain
from utils.etl_utils import get_registered_etl, get_registered_etl_from_name
from pendulum import datetime
from sqlalchemy import desc, or_

logger = logging.getLogger(__name__)

etl_dict = get_registered_etl('crawler')

session = settings.Session()

try:
    dag_start_date = datetime.strptime(
        os.getenv('AIRFLOW_START_DATE'), "%Y-%m-%d")
    AIRFLOW_START_DATE = dag_start_date
    AIRFLOW_BACKFILL = True
except Exception as e:
    AIRFLOW_START_DATE = datetime(2018, 1, 11)
    AIRFLOW_BACKFILL = False

pp = pprint.PrettyPrinter(indent=4)

default_args = {
    'owner': 'ariflow',
    # 'depends_on_past': True,
    'provide_context': True,
    'start_date': AIRFLOW_START_DATE,
    'retries': 3,
    'retry_delay': timedelta(seconds=600),
}


def call_cls_method(cls_method, *args, **kwargs):
    # next_execution_date mean doing day in ETL.
    next_date = kwargs[u'next_execution_date']
    if next_date >= datetime.now():
        next_date = datetime.now()
    kwargs[u'ds'] = next_date.strftime("%Y-%m-%d")
    kwargs[u'ds_nodash'] = next_date.strftime("%Y%m%d")
    kwargs[u'execution_date'] = next_date
    logger.info(kwargs)
    cls_method(*args, **kwargs)


def create_python_task(task, dag, pool=None):
    python_task = PythonOperator(
        task_id='{task_name}'.format(task_name=task.__name__),
        provide_context=True,
        python_callable=call_cls_method,
        op_kwargs={'cls_method': task},
        pool=pool,
        dag=dag)
    return python_task


def fix_crontime_onlyhour(cron_time):
    if len(cron_time.split(" ")) > 1:
        minute, hour, day, mon, week = cron_time.split(" ")
        # daily job
        if day == "*" and mon == "*" and week == "*" and "*" not in hour \
                and "/" not in hour:
            hour = int(hour)
            hour = hour - 8
            if hour < 0:
                hour = hour + 24
            return " ".join([minute, str(hour), day, mon, week])
    return cron_time


def trigger(context, dag_run_obj):
    logger.info(context)
    return dag_run_obj


def time_delay_trigger(context, dag_run_obj, *args, **kwargs):
    return dag_run_obj


def create_ETLDag(etl_name, cron_time, etl_task_list):
    dag_id = '{action_type}_{etl_name}'.format(
        action_type="ETL",
        etl_name=etl_name)
    cron_time = cron_time
    deploy_task_list = []

    dag = DAG(dag_id=dag_id, default_args=default_args,
              schedule_interval=cron_time,
              max_active_runs=1,
              concurrency=8,
              catchup=AIRFLOW_BACKFILL)
    deploy_task_list = [create_python_task(
        task, dag) for task in etl_task_list]

    chain(*deploy_task_list)

    return (dag_id, dag)

for etl_name, etl_class in etl_dict.items():
    etl = etl_class.createInstance()

    etl_extract_task_list = etl_class.get_extract_tasks(etl)
    etl_transform_task_list = etl_class.get_transform_tasks(etl)
    etl_load_task_list = etl_class.get_load_tasks(etl)
    etl_task_list = etl_extract_task_list + etl_transform_task_list + etl_load_task_list

    etl_cron_time = fix_crontime_onlyhour(etl.execute_cron_time)

    ETL_dag_id, ETL_dag = create_ETLDag(etl_name, etl_cron_time, etl_task_list)
    globals()[ETL_dag_id] = ETL_dag
