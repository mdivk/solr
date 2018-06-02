import os
import sys
from optparse import OptionParser
from time import sleep
from threading import Thread

# Usage:
# rindex.py -T job/flow_name.txt -D job/flow_days.txt
# log: /log/index

SOLR_URL = ''
POST_JAR_URL = 'opt/cloudera/parcels/CDH/jars/post.jar'
MAX_THREADS = 10

single_day = ''
solr_server = ''
collection = ''
json_loc_base = '/usr/indexer/'
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

    def single_rindex(self, solr_server, collection, flow_name, flow_days, json_loc):

        # print warning?
        # read solr server and compose the SOLR_URL
        SOLR_URL = 'http://' + solr_server + '.nam.nsroot.net:8983/solr/' + collection + '/update/json/docs'
        # reaad entries from flow_days
        index_command = index_command_base + SOLR_URL + ' -jar ' + POST_JAR_URL + ' ' + single_day + ' ' + json_loc
        print(index_command)
        # os.system(single_command)

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

    def worker_func(self, solr_server, collection, flow_name, flow_days, json_loc):
        sys.stdout.write("\rProcessing file " + str(i + 1) + "/" + self.total_files + '...')
        sys.stdout.flush()

        self.single_rindex(solr_server, collection, flow_name, flow_days, json_loc)


    def run(self):
        # self.options = options

        # now we have the date for the json files to be indexed

        cur_flow_days = []
        cur_flow_days = self.read_flow_days(solr_server, collection, flow_name, flow_days)

        for i, each_date in enumerate(cur_flow_days):
            # for each flow_day, start a Thread to process it, max is 10
            json_loc = json_loc_base + flow_name_loc + '/json/' + each_date
            # print(json_loc)
            self.single_rindex(solr_server, collection, flow_name, flow_days, json_loc)

        print('\nDone!')


'''     
The following code is for MultiThreading, only to be uncommented after the single Thread is working as expected:
   
        for i, each_date in enumerate(cur_flow_days):
            threads = [a for a in threads if a.isAlive()]

            while len(threads) >= MAX_THREADS:
                sleep(0.1)
                threads = [a for a in threads if a.isAlive()]

            t = Thread(target=self.worker_func, args=(input_f, i))
            threads.append(t)
            # self.logger.debug('Number of active threads: ' + str(len([a for a in threads if a.isAlive()])))           
            t.start()

        for t in threads:
            t.join()
'''

if __name__ == '__main__':
    rindex().run()
