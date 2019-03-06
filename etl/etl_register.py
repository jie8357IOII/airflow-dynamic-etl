# -*- encoding: utf8 -*-
import datetime
import inspect
import logging
import os
from functools import partial, wraps

import unicodecsv as csv

DAILY_DELAY = 10800  # 3 hours
WEEKLY_DELAY = 43200  # 12 hours
MONTHLY_DELAY = 259200  # 3 days
SEASONALLY_DELAY = 604800  # 7 days
YEARLY_DELAY = 604800  # 7 days
MAX_DELAY = 604800  # 7 days

def add_task(task_type, task_priority):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        wrapper._task_type = task_type
        wrapper._task_priority = task_priority
        return wrapper

    return decorator


logger = logging.getLogger(__name__)

extract = partial(add_task, 'extract')
transform = partial(add_task, 'transform')
load = partial(add_task, 'load')


def get_tasks(cls, task_type):
    """return task with type in ETL framework.
    each ETL framework's task should contain a type to identify task' workflow.

    Args:
        task_type (string): task type, ex: extract, transform, load.

    Returns:
        List[function]: return function which contain task_type attribute.
    """
    for _, method in inspect.getmembers(cls, predicate=inspect.ismethod):
        if hasattr(method, '_task_type') and method._task_type == task_type:
            yield method


def sort_tasks(tasks):
    """sort task with task_priority

    each task contain type & priority
    sort task with task_priority

    Args:
        tasks (List[function]): task contain type & priority

    Returns:
        List[function]: sorted tasks
    """
    return sorted(tasks, key=lambda task: task._task_priority)


class ETLRegistryHolder(type):
    """ETLBase register 

    each class inherit ETLBase will auto register itself to REGISTRY.    
    """
    REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        """overwrite __new__ for register cls

        register cls beside ETLBase itself.

        Args:
            name (string): class name
            bases (class): parent class
            attrs (dict): class attributes

        Returns:
            class: class
        """
        new_cls = type.__new__(cls, name, bases, attrs)
        if new_cls.__name__ is not 'ETLBase':
            cls.REGISTRY[new_cls.__name__] = new_cls
        return new_cls

    @classmethod
    def get_registry(cls):
        """return registered class

        Returns:
            dict: dict of classes which inherit ETLBase
        """
        return dict(cls.REGISTRY)


class ETLBase(object):
    """ETL base framework
    every ETL framework class should inherit this, 
    it use registry design pattern to get all child class
    and provide ETL register and get_task method
    """
    __metaclass__ = ETLRegistryHolder

    @classmethod
    def createInstance(cls, *arg, **kwargs):
        return cls(*arg, **kwargs)

    @classmethod
    def get_class_name(cls):
        return cls.__name__

    @staticmethod
    def register_extract(task_priority):
        """decorator, register extract type task

        Args:
            task_priority (int): priority number

        Returns:
            function: decorator
        """
        def deco(func):
            return extract(task_priority)(func)
        return deco

    @staticmethod
    def register_transform(task_priority):
        """decorator, register transform type task

        Args:
            task_priority (int): priority number

        Returns:
            function: decorator
        """
        def deco(func):
            return transform(task_priority)(func)
        return deco

    @staticmethod
    def register_load(task_priority):
        """decorator, register load type task

        Args:
            task_priority (int): priority number

        Returns:
            function: decorator
        """
        def deco(func):
            return load(task_priority)(func)
        return deco

    @staticmethod
    def get_extract_tasks(klass):
        """get sorted extract type tasks from class
        Args:
            klass (class): class which method should contain extract type

        Returns:
            List[function]: list of methods which belong extract type
        """
        return sort_tasks(get_tasks(klass, 'extract'))

    @staticmethod
    def get_transform_tasks(klass):
        """get sorted transform type tasks from class
        Args:
            klass (class): class which method should contain transform type

        Returns:
            List[function]: list of methods which belong transform type
        """
        return sort_tasks(get_tasks(klass, 'transform'))

    @staticmethod
    def get_load_tasks(klass):
        """get sorted load type tasks from class
        Args:
            klass (class): class which method should contain load type

        Returns:
            List[function]: list of methods which belong load type
        """
        return sort_tasks(get_tasks(klass, 'load'))


class ETLCrawlerRegistryHolder(ETLRegistryHolder):
    """ETLCrawler register 

    each class inherit ETLCrawler will auto register itself to REGISTRY.    
    """
    REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        """overwrite __new__ for register cls

        register cls beside ETLCrawler itself.

        Args:
            name (string): class name
            bases (class): parent class
            attrs (dict): class attributes

        Returns:
            class: class
        """
        new_cls = type.__new__(cls, name, bases, attrs)
        # new_cls = super(ETLCrawlerRegistryHolder, cls).__new__(cls, name, bases, attrs)
        if new_cls.__name__ is not 'ETLCrawler':
            super(ETLCrawlerRegistryHolder,
                  cls).REGISTRY[new_cls.__name__] = new_cls
            cls.REGISTRY[new_cls.__name__] = new_cls
        return new_cls

    @classmethod
    def get_registry(cls):
        """return registered class

        Returns:
            dict: dict of classes which inherit ETLBase
        """
        return dict(cls.REGISTRY)


class ETLCrawler(ETLBase):
    """class inherit this will auto schedule to scheduler    

    each method which registered as task will pass task_kwargs.

    task_kwargs contain
    {{ ds }}    the execution date as YYYY-MM-DD
    {{ ds_nodash }}    the execution date as YYYYMMDD
    {{ yesterday_ds }}    yesterdayâ€™s date as YYYY-MM-DD
    {{ yesterday_ds_nodash }}    yesterdayâ€™s date as YYYYMMDD
    {{ tomorrow_ds }}    tomorrowâ€™s date as YYYY-MM-DD
    {{ tomorrow_ds_nodash }}    tomorrowâ€™s date as YYYYMMDD
    {{ ts }}    same as execution_date.isoformat()
    {{ ts_nodash }}    same as ts without - and :
    {{ execution_date }}    the execution_date, (datetime.datetime)
    {{ end_date }}    same as {{ ds }}
    {{ latest_date }}    same as {{ ds }}
    """
    __metaclass__ = ETLCrawlerRegistryHolder

    execute_cron_time = "0 0 0 * *"
    retries = 3
    retry_delay_time = datetime.timedelta(hours=2)
    start_date = datetime.datetime(2017, 12, 25)
    execution_timeout = datetime.timedelta(hours=2)

    @classmethod
    def get_timedelta_delay(cls):
        try:
            minu, hour, day, mon, year = cls.execute_cron_time.split(' ')
            delay = datetime.timedelta(seconds=MAX_DELAY)
            if day == "*" and mon == "*" and year == "*":
                delay = datetime.timedelta(seconds=DAILY_DELAY)
            elif day == "*/7" and mon == "*" and year == "*":
                delay = datetime.timedelta(seconds=WEEKLY_DELAY)
            elif (mon == "*" and year == "*"):
                delay = datetime.timedelta(seconds=MONTHLY_DELAY)
            elif (mon == "*/3" and year == "*"):
                delay = datetime.timedelta(seconds=SEASONALLY_DELAY)
            elif (year == "*"):
                delay = datetime.timedelta(seconds=YEARLY_DELAY)
            return delay
        except Exception as e:
            return None

    def pre_load(self, file_dir,file_name=None, *args, **kwargs):
        """load transformed data        

        retrive data which produce from transform step

        Args:
            file_name (Str): csv file name, should pair with write_csv, 
                             default is None
            *args ([type]): [description]
            **kwargs ([type]): [description]

        Returns:
            collections.Iterable: row from file reader
        """
        filename = os.path.join(
            file_dir, self.get_class_name() + '.csv')
        if file_name:
            filename = os.path.join(
                file_dir, file_name + '.csv')
        with open(filename, 'rb') as csvfile:
            reader = csv.reader(csvfile, encoding='utf-8',
                                delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
            for row in reader:
                yield [x or None for x in row]

    def write_csv(self, file, file_dir,file_name=None, *args, **kwargs):
        """write data which encoding with utf-8

        file should be csv form.
        ex: [0,1,2,3]
        or generator

        Args:
            file (List[data]): file should be csv form.
            file_name (Str): csv file name, should pair with pre_load, 
                             default is None
        """
        filename = os.path.join(
            file_dir, self.get_class_name() + '.csv')
        if file_name:
            filename = os.path.join(
                file_dir, file_name + '.csv')
        with open(filename, 'wb') as wfile:
            wr = csv.writer(wfile, encoding='utf-8',
                            delimiter='|',
                            quoting=csv.QUOTE_NONE,
                            escapechar='\\')
            if isinstance(file, (list,)):
                logger.warn(
                    "Write List will cause memory issue, use generator plz.")
            for row in file:
                if row:  # skip "", [], None data
                    wr.writerow(row)
