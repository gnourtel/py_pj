""" Syncing Data """

import os
import threading
import time
from math import ceil
from postgres import Postgres
from usrlib import query_data

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
        print_list = [x.result for x in self.observer_list]
        print(print_list)
        time.sleep(1)

class SinglePipeline(threading.Thread):
    """ Inherit threading to spawn pulling job. Job format must be as following:
        
    """
    def __init__(self, postgresdb, mysqldb, job):
        threading.Thread.__init__(self)
        self.daemon = True
        self.postgresdb = postgresdb
        self.mysqldb = mysqldb
        self.job = job
        self.sleep_counter = 0
        self.result = ''
        self.retry = 0

    def run(self):
        self.job_run()

    def postgres_run(self, query, params):
        """ Runing query on postgresql """
        try:
            db_con = Postgres('postgresql://' + self.postgresdb)
            raw_result = db_con.all(query, {'value': params})
            result = [list(x) for x in raw_result]
            self.retry = 0
        except Exception as err:
            if self.retry <= 3:
                result = self.postgres_run(query, params)
            else:
                result = []
                self.result = '{}: get error on Postgrest DB - error {}'.format(
                    threading.current_thread().name,
                    err
                )

        return result

    def mysql_run(self, query, params, commit=False):
        """ Running query on Mysql """
        try:
            for row in range(ceil(len(params) / 1000)):
                value = params[row * 1000 : (row + 1) * 1000]
                result = query_data(self.mysqldb, query, value, is_commit=commit)
        except Exception as err:
            result = [row]
            self.result = '{}: get error on Postgrest DB - error {}'.format(
                threading.current_thread().name,
                err
            )
        return result

    def job_run(self):
        """ Main job """
        pass
