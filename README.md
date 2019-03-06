# airflow-dynamic-etl
Create dynamic-etl via registry pattern.
Let developer build ETL without touching DAG.

## Use case
### example registed ETL code
```
# register function to extract, transform, load
@ETLCrawler.register_extract(1)
    def extract(self, ds, **task_kwargs):
    ...
@ETLCrawler.register_transform(1)
    def transform(self, *args, **kwargs):
    ...
@ETLCrawler.register_load(1)
    def load(self, *args, **kwargs):
```

### example ETL DAG
```
# get etl task list
etl_extract_task_list = etl_class.get_extract_tasks(etl)
etl_transform_task_list = etl_class.get_transform_tasks(etl)
etl_load_task_list = etl_class.get_load_tasks(etl)
etl_task_list = etl_extract_task_list + etl_transform_task_list + etl_load_task_list

# create ETL DAG
ETL_dag_id, ETL_dag = create_ETLDag(etl_name, etl_cron_time, etl_task_list)
    globals()[ETL_dag_id] = ETL_dag
```

## File structure
```
.
├── LICENSE
├── README.md
├── __init__.py
├── etl
│   ├── __init__.py
│   ├── etl_register.py  # ETL register pattern
│   ├── example_crawler_etl.py # registed ETL
├── example_etl_dag.py # ETL DAG
└── utils
    ├── __init__.py
    └── etl_utils.py # utils for get ETL from etl_register
```
