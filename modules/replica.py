""" Replication Data between two DBs"""

import os
import threading
import time
import re
from math import ceil
from datetime import datetime, timedelta
from configparser import ConfigParser
import postgres
from modules import usrlib

class MainObserver(threading.Thread):
    """ Observer to display current status of all thread. All status
    is read from registered object """
    def __init__(self):
        threading.Thread.__init__(self)
        self.observer_list = []
        self.os_sc_clear = 'clear' if os.name == 'posix' else 'cls'
        self.stop_flag = False

    def run(self):
        self.print_out()

    def register(self, target):
        """ register object """
        self.observer_list.append(target)

    def print_out(self):
        """ print to the screen every 1 second """
        while self.stop_flag is False:
            os.system(self.os_sc_clear)
            print_list = [x.get_result() for x in self.observer_list]
            print('\n'.join(print_list))
            time.sleep(1)

    def stop_process(self):
        """ stop all process, invoker under Keypresserror"""
        for obs in self.observer_list:
            obs.stop_flag = True
        self.stop_flag = True

class LogIndex(object):
    """ Logging index into Ini file """
    def __init__(self, url):
        self.lock = False
        self.url = url
        self.config = ConfigParser()
        self.config.read(url)

    def write_log(self, log_id, log_num):
        """ write back into log file """
        while self.lock is True:
            pass
        self.lock = True
        self.config['Last_Index'][log_id] = str(log_num)
        with open(self.url, 'w') as writer:
            self.config.write(writer)
        self.lock = False

    def read_log(self, log_id):
        """ read from log file """
        return self.config['Last_Index'][log_id]

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
            dest_query: '',
            freqs_period: 0 => ∞
        }
        in which:
        + job_name: name of the job
        + source_db / dest_db: database string connection as format above
        + source_id: lastest indicator which will pull data has greater value than this value,
                     must be passed in as an object
        + source_type: number / datetime
        + source_query: query to get data from source.
        + freqs_period: number of second between last successful initiate and current initiate. If
                        last job is still pending at the time next initiate would be invoke, the
                        new job will be delayed until it meet the period
        + source_id will be replace by the last updated id successfully into destination
    """
    def __init__(self, job, obs, log):
        threading.Thread.__init__(self, daemon=True)
        self.job = self.validate_job(job)
        self.sleep_counter = 0
        self.result = {
            'thread_name': threading.current_thread().name,
            'status_complete': '',
            'time': 0,
            'db': '',
            'err': ''
        }
        self.retry = 0
        self.stop_flag = False
        self.obs = obs
        self.log = log

    def run(self):
        self.obs.register(self)
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
                result = row_mpl * 1000 if commit is True else []
                self.result['err'] = err

        self.retry = 0
        return result

    def job_run(self):
        """ Main job """
        while self.stop_flag is False:
            start_time = time.time()

            #Source query
            source = self.job['source_db']
            dest = self.job['dest']
            source_param = self.convert_source_id(
                self.log.read_log(self.job['source_id']),
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

            if result:
                #Insert to local
                ins_result = self.mysql_run(
                    self.convert_db(dest),
                    self.job['dest_query'],
                    result,
                    commit=True
                )
                if ins_result >= len(result):
                    self.result['status_complete'] = 'success'
                    self.result['time'] = time.time() - start_time
                    self.sleep_counter = 0
                    ins_result = len(result)
                    self.result['err'] = ''
                else:
                    self.result['status_complete'] = 'fail'
                    self.result['db'] = 'dest'

                self.log.write_log(
                    self.job['source_id'],
                    result[ins_result][self.job['source_pos']]
                )
            else:
                self.result['status_complete'] = 'fail'
                self.result['db'] = 'source'
                self.sleeping()

    def sleeping(self):
        """ sleeping trigger """
        self.sleep_counter += 10
        time.sleep(self.sleep_counter)

    @staticmethod
    def convert_source_id(source_id, time_cv=False, time_offset=0):
        """ convert source_id into correct one for query and logging"""
        if time_cv is True:
            result = datetime.fromtimestamp(source_id) - timedelta(hours=time_offset)
            result = result.strftime('%Y-%m-%d %H:%M:%S')
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

    @staticmethod
    def validate_job(job):
        """ Validate job variable if that """
        require_list = [
            'job_name',
            'source_db',
            'source_query',
            'source_id',
            'source_type',
            'source_pos',
            'dest_db',
            'dest_query',
            'freqs_period'
        ]
        if all(field in job for field in require_list):
            return job

def run(job_list, log_url):
    """ main function to run application """
    try:
        screen_update = MainObserver()
        log_file = LogIndex(log_url)
        job_threads = []
        for job in job_list:
            thread = SinglePipeline(job, screen_update, log_file)
            job_threads.append(thread)
            thread.start()
    except KeyboardInterrupt:
        for running_job in job_threads:
            running_job.join()
        screen_update.stop_process()
        print("\nExitting...")
