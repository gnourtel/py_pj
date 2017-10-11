""" TMS Pulling Data """

import threading
import postgres
from usrlib import query_data

class observer(threading.Thread):
    observer_list = []
    def __init__(self):
        threading.Thread.__init__(self)

    def register(self, target):
        self.observer_list.insert(target)

    def print_out(self):
        print_list = [x.result for x in self.observer_list]
        print(print_list, end='\r', flush=True)

class singlePipeline(threading.Thread):
    """ Inherit threading to spawn pulling job """
    def __init__(self, postgresdb, job):
        threading.Thread.__init__(self)
        self.daemon = True
        self.postgresdb = postgresdb
        self.job = job
        self.sleep_counter = 0
        self.result = ''

    def run(self):
        pass

    def postgres_run(self, query, par)
