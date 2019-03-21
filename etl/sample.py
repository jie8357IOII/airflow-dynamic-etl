import errno
import logging
import os
import random
import sys
import time

from etl_register import ETLCrawler

class sample_ETL(ETLCrawler):

    execute_cron_time = "00 20 * * *"

    @ETLCrawler.register_extract(1)
    def extract_something(self, ds, **task_kwargs):
        print('I am something')

    @ETLCrawler.register_extract(2)
    def extract_otherthing(self, ds, **task_kwargs):
        print('otherthing')

    @ETLCrawler.register_extract(3)
    def extract_anotherthing(self, ds, **task_kwargs):
        print('anotherthing')


    @ETLCrawler.register_transform(1)
    def transform1(self, *args, **kwargs):
        print('transform1 by order 1')
        
    @ETLCrawler.register_transform(3)
    def transform2(self, *args, **kwargs):
        print('transform2 by order 3')

    @ETLCrawler.register_transform(2)
    def transform3(self, *args, **kwargs):
        print('transform3 by order 2')

    @ETLCrawler.register_load(1)
    def load(self, *args, **kwargs):
        print('do load')