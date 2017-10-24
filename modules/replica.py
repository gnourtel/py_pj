""" Syncing Data """

import os
import threading
import time
import re
from math import ceil
from datetime import datetime
from configparser import ConfigParser
import postgres
import usrlib

class MainObserver(threading.Thread):
    """ Observer to display current status of all thread. All status
    is read from registered object """
    def __init__(self):
        threading.Thread.__init__(self)
        self.observer_list = []

    def run(self):
        self.print_out()

    def register(self, target):
        """ register object """
        self.observer_list.insert(target)

    def print_out(self):
        """ print to the screen every 1 second """
        if os.name == 'posix':
            os.system('clear')
        else:
            os.system('cls')
        print_list = [x.get_result() for x in self.observer_list]
        print('\n'.join(print_list))
        time.sleep(1)

    def stop_process(self):
        """ stop all process, invoker under Keypresserror"""
        for obs in self.observer_list:
            obs.stop_flag = True

class SinglePipeline(threading.Thread):
    """ Inherit threading to spawn pulling job. Job format must be a dict with following field
        as following:
        {
            job_name: '',
            source_db: 'postgresql/mysql' + '://[user]:[pass]@[host(:port)]/[database]',
            source_query: '',
            source_id: '',
            source_type: '',
            source_pos: '',
            dest_db: 'postgresql/mysql',
            dest_table: '',
            dest_insert_mode: 'insert/insert-rmd',
            freqs_period: 0 => ∞
        }
        in which:
        + job_name: name of the job
        + source_db / dest_db: database string connection as format above
        + source_id: lastest indicator which will pull data has greater value than this value,
                     must be passed in as an object
        + source_type: number / datetime
        + source_query: query to get data from source. Must be formatted accordingly because
                        this will be the standard to create insert query:
                        * source and destination columns must be in same name or
                        * all alias must be after ' AS ' string and same with destination
                        * all column must be in first select and from
        + dest_insert_mode: must be either insert or insert-rmd (remove_duplicate)
        + freqs_period: number of second between last successful initiate and current initiate. If
                        last job is still pending at the time next initiate would be invoke, the
                        new job will be delayed until it meet the period
        + source_id will be replace by the last updated id successfully into destination
    """
    def __init__(self, job):
        threading.Thread.__init__(self)
        self.daemon = True
        self.job = {
            'job_name': job['job_name'],
            'source_db': job['source_db'],
            'source_query': job['source_query'],
            'source_id': job['source_id'],
            'source_type': job['source_type'],
            'source_pos': job['source_pos'],
            'dest_db': job['dest_db'],
            'dest_table': job['dest_table'],
            'dest_insert_mode': job['dest_insert_mode'],
            'freqs_period': job['freqs_period']
        }
        self.sleep_counter = 0
        self.result = {
            'thread_name': threading.current_thread().name,
            'status_complete': 0,
            'db': '',
            'err': ''
        }
        self.retry = 0
        self.stop_flag = False

    def run(self):
        self.job_run()

    def get_result(self):
        """ fetch result into string """
        if self.result['err'] != '':
            result = '{}: {} got error in db {} - error: {} - sleeping {} s'.format(
                self.result['thread_name'],
                self.result['job_name'],
                self.result['db'],
                self.result['err'],
                self.sleep_counter
            )
            
        elif self.result['status_complete'] != 0:
            result = '{}: {} is complete task in {} s'.format(
                self.result['thread_name'],
                self.result['job_name'],
                self.result['status_complete']
            )
        else:
            result = '{}: {} is sleeping for {} s'.format(
                self.result['thread_name'],
                self.result['job_name'],
                self.sleep_counter
            )
        return result

    def postgres_run(self, postgresdb, query, params):
        """ Runing query on postgresql """
        try:
            db_con = postgres.Postgres('postgresql://' + postgresdb)
            raw_result = db_con.all(query, {'value': params})
            result = [list(x) for x in raw_result]
        except postgres.psycopg2.OperationalError as err:
            if self.retry <= 3:
                self.retry += 1
                result = self.postgres_run(postgresdb, query, params)
            else:
                result = []
                self.result['err'] = err
        except postgres.psycopg2.ProgrammingError as err:
            result = []
            self.result['err'] = err

        self.retry = 0
        return result

    def mysql_run(self, mysqldb, query, params, commit=False):
        """ Running query on Mysql """
        try:
            for row_mpl in range(ceil(len(params) / 1000)):
                value = params[row_mpl * 1000 : (row_mpl + 1) * 1000]
                result = usrlib.query_data(mysqldb, query, value, is_commit=commit)
        except usrlib.mysql.connector.errors.InterfaceError as err:
            if self.retry <= 3:
                self.retry += 1
                result = self.mysql_run(query, mysqldb, value, commit=commit)
            else:
                result = [row_mpl * 1000]
                self.result = '{}: get error on MySQL DB - cnn error {}'.format(
                    threading.current_thread().name,
                    err
                )

        self.retry = 0
        return result

    def job_run(self):
        """ Main job
        {
            job_name: '',
            source_db: 'postgresql/mysql' + '://[user]:[pass]@[host(:port)]/[database]',
            source_query: '',
            source_id: '',
            source_type: '',
            source_pos: '',
            dest_db: 'postgresql/mysql',
            dest_insert_mode: 'insert/insert-rmd',
            freqs_period: 0 => ∞
        }
        """

        while self.stop_flag is False:
            start_time = time.time()
            #Source query
            source = self.job['source_db']
            source_param = self.convert_source_id(
                self.job['source_id'],
                self.job['source_type'] == 'datetime')
            if 'mysql' in source:
                result = self.mysql_run(
                    self.convert_db(source),
                    self.job['source_query'],
                    source_param
                )
            else:
                result = self.postgres_run(
                    source,
                    self.job['source_query'],
                    source_param
                )

            if self.result['err'] == '':
                #insert query
                self.sleep_counter = 0
                self.result['status_complete'] = time.time() - start_time
            else:
                self.sleep_counter += 10
                time.sleep(self.sleep_counter)

    @staticmethod
    def convert_source_id(source_id, time_cv=False):
        """ convert source_id into correct one for query and logging"""
        if time_cv is True:
            result = datetime.fromtimestamp(source_id)
        elif isinstance(source_id, datetime):
            result = datetime.timestamp(source_id)
        else:
            result = source_id
        return result

    @staticmethod
    def convert_db(db_string):
        """ switch from format into dict used by mysql """
        user, pwd, host, _, dtb = re.match(
            'mysql://(.*?):(.*?)@(.*?)(|:.*?)/(.*)',
            db_string
        )
        return {
            'user': user,
            'password': pwd,
            'host': host,
            'database': dtb
        }
