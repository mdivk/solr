import os
from optparse import OptionParser
from time import sleep
import datetime
from threading import Thread, current_thread, Lock
import threading
import time
import logging
import getpass
import socket

# Usage:
# rindex.py -T job/flow_name.txt -D job/flow_days.txt
# log: /log/index

SOLR_URL = ''
POST_JAR_URL = 'opt/cloudera/parcels/CDH/jars/post.jar'
DOMAIN = '.nam.nsroot.net:8983/solr/'

MAX_THREADS = 10

single_day = ''
solr_server = ''
collection = ''
json_loc_base = '/usr/indexer/data/'
#json_loc_base = 'json/citifix/'  # note, the json loc base is to be replaced by the commented one when tested in work

json_loc = ''
index_command_base = 'java -Dtype=application/json -Drecursive -Durl='
index_command = ''
solr_instance_detail = ''
flow_name = ''
flow_days = ''
flow_name_loc = ''
json_list = []


class rindex():
    def __init__(self, solr_server=None, collection=None, flow_name=None, flow_days=None):
        self.json_file = ''
        self.process_options(solr_server, collection, flow_name, flow_days)
        self.config_logger()

    def config_logger(self):
        global logger_location
        logger_location = os.getcwd() + '\\log\\' + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '.log'
        print("logger_location:" + logger_location)
        logging.basicConfig(filename=logger_location, format='%(asctime)s %(levelname)s %(message)s')
        self.logger = logging.getLogger('index_log')
        self.logger.setLevel(logging.DEBUG)

    def process_options(self, the_solr_server, the_collection, the_flow_name, the_flow_days):

        parser = OptionParser()
        parser.add_option("-s", "--solr_instance_detail", dest="solr_instance_detail",
                          help="Enter solr_instance_detail")
        # parser.add_option("-c", "--collection", dest="collection", help="Enter solr collection")
        parser.add_option("-f", "--flow_name", dest="flow_name", help="Enter flow_name file")
        parser.add_option("-d", "--flow_days", dest="flow_days", help="enter flow_day file")

        (options, args) = parser.parse_args()

        if not options.solr_instance_detail:
            if not the_solr_server:
                parser.error('solr_server not provided (-s solr_server)')
        else:
            with open(options.solr_instance_detail) as f:
                solr_instance_detail = f.readlines()

            global solr_server
            global collection
            solr_server = solr_instance_detail[0].split('|')[0]
            collection = solr_instance_detail[0].split('|')[1]

        if not options.flow_name:
            if not the_flow_name:
                parser.error('flow name not provided (-f option)')
        else:
            global flow_name
            flow_name = options.flow_name

        if not options.flow_days:
            if not the_flow_days:
                parser.error('flow days not provided (-d option)')
        else:
            global flow_days
            flow_days = options.flow_days

    def get_json_files(self, json_loc):
        '''
        Get list of json files in json_loc
        '''
        files_list = []

        for x in os.walk(json_loc):
            files_list.append(x)

        return files_list

    def get_total_file_num(self, folder):

        return len([name for name in os.listdir("data/citifix/apac/apeq/" + folder) if os.path.isfile(os.path.join("data/citifix/apac/apeq/" + folder, name))])

    def read_flow_name(self, flow_name):
        global flow_name_loc
        with open(flow_name) as f:
            c = f.readlines()
            # you may also want to remove whitespace characters like `\n` at the end of each line
        c = [x.strip() for x in c]
        flow_name_loc = c[0]

        return flow_name_loc

    def read_flow_days(self, solr_server, collection, flow_name, flow_days):
        with open(flow_days) as f:
            c = f.readlines()
        # you may also want to remove whitespace characters like `\n` at the end of each line
        flow_days = [x.strip() for x in c]

        return flow_days

    def files(self, path):
        for file in os.listdir(path):
            if os.path.isfile(os.path.join(path, file)):
                yield file

    # the worker is to process the assigned folder
    def worker_func(self, solr_server, collection, flow_name, flow_days, json_loc, i, lock):
        lock.acquire()
        print("Current thread: " + threading.current_thread().getName())
        # read solr server and compose the SOLR_URL
        SOLR_URL = 'http://' + solr_server + '.domain.net:8983/solr/' + collection + '/update/json/docs'
        # reaad entries from folder of flow_days

        for json_file in self.files(json_loc):
            self.logger.info('Processing: ' + json_file)
            index_command = "\r" + index_command_base + SOLR_URL + ' -jar ' + POST_JAR_URL + ' ' + single_day + ' ' + json_loc + '/' + json_file
            print(index_command)

        lock.release()


    # The following code is for MultiThreading, only to be uncommented after the single Thread is working as expected:
    # Multi threading is created based on each day in the flow_days, each day will get a Thread assigned to it
    def run(self):

        start_time = datetime.datetime.now()

        cur_flow_days = []
        cur_flow_days = self.read_flow_days(solr_server, collection, flow_name, flow_days)

        total_files = 0
        sub = 0
        for i, each_flow_day in enumerate(cur_flow_days):
            sub = self.get_total_file_num(each_flow_day)
            total_files = total_files + sub

        cur_flow_name = ''
        cur_flow_name = self.read_flow_name(flow_name)

        lock = Lock()

        threads = []

        for i, each_date in enumerate(cur_flow_days):
            threads = [a for a in threads if a.isAlive()]

            while len(threads) >= MAX_THREADS:
                sleep(3)
                threads = [a for a in threads if a.isAlive()]

            json_loc = json_loc_base + flow_name_loc + '/' + each_date

            t = Thread(target=self.worker_func, args=[solr_server, collection, flow_name, flow_days, json_loc, i, lock])
            threads.append(t)

            t.start()

        for t in threads:
            t.join()

        stop_time = datetime.datetime.now()
        elapsed_time = stop_time - start_time

        self.logger.info('====================================Indexing Report====================================')
        self.logger.info('Time the indexing started: ' + str(start_time))
        self.logger.info('Time the indexing ended: ' + str(stop_time))
        self.logger.info('Time the indexing costs: ' + str(elapsed_time))
        self.logger.info('Solr Server: ' + solr_server)
        self.logger.info('Solr Collection Name: ' + collection)
        self.logger.info('Solr URL: ' + SOLR_URL)
        self.logger.info('Index Location: ' + self.options.input_path)  #To be updated with the HDFS location from the SOLR Overview for the collection
        self.logger.info('Total Documents to be indexed: ' + str(total_files))
#        self.logger.info('Total Documents indexed: ' + str(idx))
#        self.logger.info('Total Documents skipped: ' + str(total_skipped))
        self.logger.info('Log location for this session: ' + logger_location)
        self.logger.info('Job/report processed by: ' + getpass.getuser() + ' on host: ' + socket.gethostname())
        self.logger.info('=============================================================================================')


if __name__ == '__main__':
    rindex().run()
