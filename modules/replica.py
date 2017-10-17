""" Syncing Data """

import os
import threading
import time
from math import ceil
import postgres
import usrlib

class MainObserver(threading.Thread):
    """ Observer to display current status of all thread. All status
    is read from registered object """
    observer_list = []
    def __init__(self):
        threading.Thread.__init__(self)

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
        print_list = [x.get_result for x in self.observer_list]
        print(print_list)
        time.sleep(1)

class SinglePipeline(threading.Thread):
    """ Inherit threading to spawn pulling job. Job format must be a dict with following field
        as following:
        {
            job_name: '',
            source_db: 'postgresql/mysql' + '://[user]:[pass]@[host]([:port])/[database]',
            source_query: '',
            source_id: '',
            source_type: '',.
            dest_db: 'postgresql/mysql',
            dest_query: '',
            dest_pos: ''
            freqs: 0 => ∞
        }
    """
    def __init__(self, postgresdb, mysqldb, job):
        threading.Thread.__init__(self)
        self.daemon = True
        self.postgresdb = postgresdb
        self.mysqldb = mysqldb
        self.job = job
        self.sleep_counter = 0
        self.result = {
            'thread_name': threading.current_thread().name,
            'status_complete': 0,
            'db': '',
            'err': ''
        }
        self.retry = 0

    def run(self):
        self.job_run()

    def get_result(self):
        """ fetch result into string """
        if self.sleep_counter != 0:
            result = '{}: {} is sleeping for {} s'.format(
                self.result['thread_name'],
                self.result['job_name'],
                self.sleep_counter
            )
        elif self.result['status_complete'] != 0:
            result = '{}: {} is complete task in {}'.format(
                self.result['thread_name'],
                self.result['job_name'],
                self.result['status_complete']
            )
        else:
            result = '{}: {} got error in db {} - error: {}'.format(
                self.result['thread_name'],
                self.result['job_name'],
                self.result['db'],
                self.result['err']
            )
        return result

    def postgres_run(self, query, params):
        """ Runing query on postgresql """
        try:
            db_con = postgres.Postgres('postgresql://' + self.postgresdb)
            raw_result = db_con.all(query, {'value': params})
            result = [list(x) for x in raw_result]
        except postgres.psycopg2.OperationalError as err:
            if self.retry <= 3:
                self.retry += 1
                result = self.postgres_run(query, params)
            else:
                result = []
                self.result['err'] = err
        except postgres.psycopg2.ProgrammingError as err:
            result = []
            self.result['err'] = err

        self.retry = 0
        return result

    def mysql_run(self, query, params, commit=False):
        """ Running query on Mysql """
        try:
            for row in range(ceil(len(params) / 1000)):
                value = params[row * 1000 : (row + 1) * 1000]
                result = usrlib.query_data(self.mysqldb, query, value, is_commit=commit)
        except usrlib.mysql.connector.errors.InterfaceError as err:
            if self.retry <= 3:
                self.retry += 1
                result = self.mysql_run(query, value, commit=commit)
            else:
                result = [row]
                self.result = '{}: get error on MySQL DB - cnn error {}'.format(
                    threading.current_thread().name,
                    err
                )

        self.retry = 0
        return result

    def job_run(self):
        """ Main job """
        pass
