import bz2
import datetime
import errno
import getpass
import hashlib
import json
import logging
import os
import re
import socket
import sys
import shutil
from collections import namedtuple
from glob import glob
from optparse import OptionParser

from lxml import etree

from values import values, tags

# to do:
# save json to the right folder
# move json accordingly

# generate the dictionary from the seperate values.py (look up table)
names = dict(zip(tags, values))
target_bz2 = ""
output_f = ""

# define a staging folder for recon purpose
output_f_staging = ""

# define the staging folder and final folder's name
staging_folder = ""
final_folder = ""

# define a list for keeping the history purpose
history = namedtuple("History", ("archive", "json"))
processing_history = []

# define logger location
logger_location = ""

# define the recon metric variables
mgs_in_cur_bz = 0
json_file_cnt = 0
idx_of_bz2 = 0
raw_json_file_cnt = 0
total_bz_files = 0
total_json_file = 0

class XMLToJson():
    def __init__(self, msgtype=None, region=None, flow=None, path=None, output=None, debug=None):
        '''
        Create the XMLToJson Object
        constructor parameters can be omitted if the commandline parameters are passed
        Usage: python2.7 xml_to_json.py -r xregion -f xflow -p input_path -o output_path
        -p input_path can be a directory or an individual .bz2 file
        -o output_path is optional. default value is 'json'
        '''

        self.json_data = []
        self.process_options(msgtype, region, flow, path, output, debug)
        #output_f = os.path.join(self.options.output_path, self.options.input_path.strip('/'))
        output_f = self.options.output_path
        self.create_log_directory()
        # print('init: logger_location:' + logger_location)
        self.config_logger()
        # print('init2: logger_location:' + logger_location)
        self.create_path_if_not_exists(output_f)
        self.logger_location = ''

    def usage(self):
        print('Usage:')
        print('msg2json -d <true/false> -t <fix/xml> -r <region name> -f <flow name> -p <folder of bz2 file(s) or single file>, -o <folder of json file(s)')

    def process_options(self, msgtype, region, flow, path, output, debug):
        '''
        Process the Options that the user provided
        either via commandline or
        as constructor parameters.
        '''

        parser = OptionParser()
        parser.add_option("-t", "--msgtype", dest="msgtype", help="Enter msgtype", metavar="PATH")
        parser.add_option("-r", "--region", dest="region", help="Enter region", metavar="PATH")
        parser.add_option("-f", "--flow", dest="flow", help="enter flow", metavar="PATH")
        parser.add_option("-p", "--path", dest="input_path", help="bz2 file path", metavar="PATH")
        parser.add_option("-o", "--output", dest="output_path", help="json file path", metavar="OUTPUT")
        parser.add_option("-d", "--debug", dest="debug", help="don't print status messages to stdout")

        (options, args) = parser.parse_args()

        if not options.msgtype:
            parser.error('msgtype not provided (-t option)')
            self.usage()
        else:
            if options.msgtype != "fix" and options.msgtype != "xml":
                parser.error('msgtype can be only fix or xml at the moment, please correct.')
                self.usage()
                exit(2)

        if not options.region:
            if not region:
                parser.error('region not provided (-r option)')
                self.usage()
            else:
                options.region = region

        if not options.flow:
            if not flow:
                parser.error('flow value not provided (-f option)')
                self.usage()
            else:
                options.flow = flow

        if not options.input_path:
            if not path:
                parser.error('input path not provided (-p option)')
                self.usage()
            else:
                options.input_path = path

        if not os.path.exists(options.input_path):
            parser.error('input path does not exist (-p option)')
            self.usage()

        options.input_path = options.input_path.rstrip('/').rstrip('\\')

        if not options.output_path:
            options.output_path = 'json'

        if not os.path.exists(options.output_path) or not os.path.isdir(options.output_path):
            os.makedirs(options.output_path)

        if options.debug != "True" and options.debug != "False":
            parser.error('debug option can be only True or False, please correct.')
            self.usage()
            exit(2)

        self.options = options

        if os.path.isfile(options.input_path):
            self.options.is_file = True
        else:
            self.options.is_file = False

    def config_logger(self):
        global logger_location
        logger_location = os.getcwd() + '/log/' + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '.log'
        print('logger_location:' + logger_location)
        logging.basicConfig(filename=logger_location, format='%(asctime)s %(levelname)s %(message)s')
        self.logger = logging.getLogger('xml_to_json_log')
        self.logger.setLevel(logging.DEBUG)

    def create_path_if_not_exists(self, path):
        '''
        If the path doesn't exist, create the path (all directories in the given path)
        '''
        if not os.path.exists(path):
            try:
                self.logger.info('Creating path ' + os.path.dirname(path))
                os.makedirs(path)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

    def create_log_directory(self):
        if not os.path.exists(os.getcwd() + '\\log'):
            try:
                print('create_log_directory:' + str(os.getcwd()) + '/log')
                os.makedirs(os.getcwd() + '/log')
            except OSError as ex:
                print('create_log_directory:' + str(ex))
                pass

    def save_json_file(self, f, data):
        with open(f, 'w') as fp:
            json.dump(data, fp)
        self.logger.info('Saved ' + f)

    def move_data(self, sourse_folder, target_folder):
        for filename in os.walk(self.options.input_path):
            for y in glob(os.path.join(filename[0], '*.json')):
                try:
                    shutil.move(filename, target_folder)
                except Exception:
                    pass

    def save_data(self, json_data, file_base_name, output_f):
        global processing_history
        global json_file_cnt
        if not json_data:
            return

        if isinstance(json_data, (list,)):
            json_file_name = ''

            for i, each_json_data in enumerate(json_data):
                json_file_name = output_f + "\\" + file_base_name + '_' + str(i + 1) + '.json'
                source_bz = json_file_name.replace("staging", "")
                # add a new item to processing_history for detailed logging purpose
                processing_history.append(history(output_f, json_file_name))
                json_file_cnt += 1
                self.save_json_file(json_file_name, each_json_data)
            return i + 1
        else:
            self.save_json_file(output_f + "\\" + file_base_name + '.json', json_data)
            return 1

    def parseXML(self, xml_data, fname):
        '''
        Parse the given XML data, extract the required info and
        return a dict object with some hardcod-ed custom values
        '''
        root = etree.fromstring(xml_data)

        data = {'ID': '', 'UITID': '', 'TRD_DATE': '', 'MESSAGE_ID': '', 'REGION': self.options.region,
            'FLOW': self.options.flow, 'PATH': fname}

        citimlTradeNotificationMessageHeader = root.find('citiml:citimlTradeNotificationMessageHeader', root.nsmap)
        citimlMessageId = citimlTradeNotificationMessageHeader.find('citiml:citimlMessageId', root.nsmap)
        if citimlMessageId is not None:
            data['MESSAGE_ID'] = citimlMessageId.text
            # generate the digest for the entire message
            # data['ID'] = hashlib.sha1(citimlMessageId.text).hexdigest()
            data['ID'] = hashlib.sha1(xml_data).hexdigest()

        citimlNotificationBundleDetails = root.find('citiml:citimlNotificationBundleDetails', root.nsmap)
        citimlPostEventTrades = citimlNotificationBundleDetails.find('citiml:citimlPostEventTrades', root.nsmap)

        for citimlPostEventTrade in citimlPostEventTrades:
            postEventTrade = citimlPostEventTrade.find('citiml:postEventTrade', root.nsmap)

            tradeHeader = postEventTrade.find('fpml:tradeHeader', root.nsmap)
            tradeDate = tradeHeader.find('fpml:tradeDate', root.nsmap)
            if tradeDate is not None and data['TRD_DATE'] == '':
                data['TRD_DATE'] = tradeDate.text

            for partyTradeIdentifier in tradeHeader:
                tradeIds = partyTradeIdentifier.findall('.//fpml:tradeId', root.nsmap)

                for tradeId in tradeIds:
                    if tradeId is not None and tradeId.attrib[
                        'tradeIdScheme'] == 'http://www.dtcc.com/internal-reference-id' and data['UITID'] == '':
                        data['UITID'] = tradeId.text
                    elif tradeId is not None and tradeId.attrib['tradeIdScheme'] == 'UniqueInternalTradeID' and data[
                        'UITID'] == '':
                        data['UITID'] = tradeId.text
        return data

    def get_fix_line_count(self, filename):
        global mgs_in_cur_bz
        try:
            file_data = bz2.BZ2File(filename, 'rb')

            # get the number of fix messages in the current bz2 file
            if self.options.msgtype == 'fix':
                mgs_in_cur_bz = len(file_data.readlines())

            return mgs_in_cur_bz
        except:
            self.logger.error('Error while reading ' + filename)
            return 0

    def read_file(self, filename):
        try:
            file_data = bz2.BZ2File(filename, 'rb')

            return file_data.read()
        except:
            self.logger.error('Error while reading ' + filename)

    def process_file(self, msgtype, f):
        '''
        Process a bz2 file
        returns either a dict object or a list of dict objects
        list of dict objects when there are 'multiple' xml file contents in the specified file.
        '''
        self.logger.info('Processing ' + f)
        if msgtype == 'xml':
            d = self.process_xml_file(f)
        else:

            d = self.process_fix_file(f)

        return d

    def parseFIX(self, fix_data, fname):
        '''
        Parse the given XML data, extract the required info and
        return a dict object with some hardcod-ed custom values
        '''

        data = {'id': '', 'region': '', 'flow': '', 'path': '', 'order_id': '', 'trade_date': ''}

        data['id'] = hashlib.sha1(fix_data).hexdigest()

        fix_split = fix_data.strip().replace('\x02', '').split('\x01')
        dict_data = dict(map(lambda items: (names.get(items.split('=')[0], items.split('=')[0]), items.split('=')[1]),
                             filter(None, fix_split)))

        data['region'] = self.options.region
        data['flow'] = self.options.flow
        data['path'] = fname
        data['order_id'] = dict_data.get('OrderID')
        data['trade_date'] = dict_data.get('TradeDate')

        return data

    def process_fix_file(self, f):

        delimiter1 = '8=FIX.4.4'

        self.logger.info('Processing ' + f)
        file_data = self.read_file(f).replace('\n', '')

        if file_data.count(delimiter1) <= 1:
            try:
                print('   Processing message 1/1 in bz2 file ' + str(idx_of_bz2 + 1) + "/" + str(total_bz_files) + ': ' + f)
                return self.parseFIX(file_data, f)
            except:
                self.logger.error('Error while parsing the FIX file' + f)
                return {}
        else:
            d = []
            idx_of_fix = 1
            for fix_data in file_data.split(delimiter1):
                if not fix_data:
                    continue
                try:
                    print('   Processing message ' + str(idx_of_fix) + '/' + str(raw_json_file_cnt) + ' in bz2 file ' + str(
                        idx_of_bz2 + 1) + "/" + str(total_bz_files) + ': ' + f)
                    d.append(self.parseFIX(delimiter1 + fix_data, f))
                except:
                    pass

                idx_of_fix += 1

        return d

    # return list of dictionary object
    def process_xml_file(self, f):

        delimiter1 = '<?xml version="1.0" encoding="UTF-8"'

        self.logger.info('Processing ' + f)
        file_data = self.read_file(f).replace('\x02', '')
        file_data = re.sub(r'<citiml:citimlTradeNotification\s+xmlns', '<citiml:citimlTradeNotification xmlns',
                           file_data)

        if file_data.count(delimiter1) == 0 and file_data.count('<citiml:citimlTradeNotification xmlns') > 1:
            file_data = re.sub(r'<citiml:citimlTradeNotification xmlns',
                               '<?xml version="1.0" encoding="UTF-8"?><citiml:citimlTradeNotification xmlns', file_data)

        if file_data.count(delimiter1) <= 1:
            try:
                return self.parseXML(file_data, f)
            except:
                self.logger.error('Error while parsing the XML file' + f)
                return {}
        else:
            d = []
            for xml_data in file_data.split(delimiter1):
                if not xml_data:
                    continue
                try:
                    d.append(self.parseXML(delimiter1 + xml_data, f))
                except:
                    pass
        return d

    def get_bz2_files_count(self):
        '''
        Get the count of bz2 files in options.input_path
        '''
        count = 0

        if os.path.isfile(self.options.input_path):
            count = 1
        else:
            for x in os.walk(self.options.input_path):
                for y in glob(os.path.join(x[0], '*.bz2')):
                    count += 1

        return count

    def get_bz2_files(self):
        '''
        Get list of bz2 files in options.input_path
        '''
        files_l = []

        if self.options.is_file:
            files_l.append(self.options.input_path)
        else:
            for x in os.walk(self.options.input_path):
                for y in glob(os.path.join(x[0], '*.bz2')):
                    files_l.append(y)

        return files_l

    def emptify_staging(self, folder):
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                    # elif os.path.isdir(file_path): shutil.rmtree(file_path)
            except Exception as e:
                print(e)

    def run(self):
        start_time = datetime.datetime.now()
        global json_file_cnt
        global idx_of_bz2
        global raw_json_file_cnt
        global total_bz_files
        global output_f
        global total_json_file

        raw_json_file_cnt = 0
        total_json_file = 0

        total_bz_files = str(self.get_bz2_files_count())

        idx_of_bz2 = 1
        total_skipped = 0

        for idx_of_bz2, input_f in enumerate(self.get_bz2_files()):
            print("\rProcessing bz2 file " + str(idx_of_bz2 + 1) + "/" + total_bz_files + ': ' + input_f)

            self.logger.debug(
                'Output file path derived by joining Output directory: ' + self.options.output_path + ' and, ')
            self.logger.debug('input bz2 filename - ' + input_f.replace(os.path.dirname(self.options.input_path),
                                                                        '') + ' by removing the  parent folderpath -' + os.path.dirname(
                self.options.input_path) + ' from input bz2 file path - ' + input_f)

            # get the direct count by lines for the bz2 file
            raw_json_file_cnt = self.get_fix_line_count(input_f)

            try:

                json_data = self.process_file(self.options.msgtype, input_f)

                # if input is single file, the file's name is to be used as part of the json file's name
                if self.options.is_file:
                    output_f = output_f + self.options.output_path + "\\final"
                else:
                    output_f = os.path.join(self.options.output_path + "\\final",
                                            input_f.replace(self.options.input_path, '').strip('\\').strip('/'))

                # output_f_staging = os.path.join(self.options.output_path + "\\staging", os.path.basename(input_f))
                output_f_staging = output_f.replace("final", "staging")

                self.create_path_if_not_exists(output_f_staging)

                staging_folder = os.path.dirname(output_f_staging)
                staging_folder = output_f_staging

                self.create_path_if_not_exists(output_f_staging)

                inputfile_base_name = os.path.basename(input_f)
                outputfile_base_name = os.path.basename(output_f_staging)

                # Create the staging folder and save the result to it, and get the json file count
                self.create_path_if_not_exists(output_f_staging)
                json_file_cnt += self.save_data(json_data, inputfile_base_name, output_f_staging)

                # only if the above two counts match, then move the output_f_staging to the final folder output_f

                #Recon starts here: applies to only FIX format
                if json_file_cnt == raw_json_file_cnt and self.options.msgtype == 'fix':
                    self.create_path_if_not_exists(output_f)
                    final_folder = output_f

                    for i in os.listdir(staging_folder):
                        if not os.path.exists(final_folder):
                            shutil.move(os.path.join(staging_folder, i), final_folder)
                        else:
                            shutil.copy2(os.path.join(staging_folder, i), final_folder)

                    # After the move, json_file_cnt needs to be reset for the next bz file
                    total_json_file += json_file_cnt
                    json_file_cnt = 0
                    raw_json_file_cnt = 0

                    idx_of_bz2 += 1

                else:
                    if self.options.msgtype == 'fix':
                        self.logger.error('Recon error on the file ' + input_f)
                        self.logger.error('Number of messages extracted from bz file: ' + str(raw_json_file_cnt))
                        self.logger.error('Json generated from bz file is ' + str(json_file_cnt))
                        total_skipped += 1
                    else:
                        self.create_path_if_not_exists(output_f)
                        final_folder = output_f

                        for i in os.listdir(staging_folder):
                            if not os.path.exists(final_folder):
                                shutil.move(os.path.join(staging_folder, i), final_folder)
                            else:
                                shutil.copy2(os.path.join(staging_folder, i), final_folder)

                        # After the move, json_file_cnt needs to be reset for the next bz file
                        total_json_file += json_file_cnt
                        json_file_cnt = 0
                        raw_json_file_cnt = 0

                        idx_of_bz2 += 1

                print("\rProcessed bz2 file " + str(idx_of_bz2) + "/" + total_bz_files + ': ' + input_f)
                #Recon ends here

            except Exception as ex:
                total_skipped += 1
                self.logger.error('Error while parsing the file ' + input_f + ' Details: ' + str(ex))
                pass

        #empty the staging folder
        self.emptify_staging(staging_folder)


        if self.options.debug == "True":
            print("Starting writting history to the log, time cost could be high as the debug option is set to True, the total json files generated is " + str(total_json_file))
        else:
            print("Starting writting history to the log, the log won't include a detailed report as the debug option is set to False, the total json files generated is " + str(total_json_file))

        stop_time = datetime.datetime.now()
        elapsed_time = stop_time - start_time

        self.logger.info(
            '=============================================================================================')
        self.logger.info('Report for the session')
        self.logger.info('Time the session started: ' + str(start_time))
        self.logger.info('Time the session ended: ' + str(stop_time))
        self.logger.info('Time the session costs: ' + str(elapsed_time))
        self.logger.info('Data Type processed in this session: ' + self.options.msgtype)
        self.logger.info('Data Region processed in this session: ' + self.options.region)
        self.logger.info('Data Flow processed in this session: ' + self.options.flow)
        self.logger.info('Data processed in this session: ' + self.options.input_path)
        self.logger.info('Result generated in this session: ' + self.options.output_path)
        self.logger.info('Total bz2 archives to be processed: ' + str(total_bz_files))
        self.logger.info('Total bz2 archives processed: ' + str(idx_of_bz2))
        self.logger.info('Total bz2 archives failed: ' + str(total_skipped))
        self.logger.info('Total completed json outputs: ' + str(total_json_file))

        if self.options.debug == "True":
            self.logger.info(
                '******************************** Detailed processing history -- Start ***********************')

            print ("Beginning generating detailed log, the time varies depending on the number of json files generated ")
            print ("Total Number of JSON Files Generated: " + str(total_json_file))
            previous_archive = ""
            for history in enumerate(processing_history):
                archive = history[1][0]
                if archive <> previous_archive:
                    # self.logger.info('\n')
                    self.logger.info('Archive location: ' + history[1][0])
                    previous_archive = history[1][0]
                self.logger.info('Json output: ' + history[1][1].replace("staging", "final"))

            self.logger.info(
                '********************************* Detailed processing history -- End ************************')
        else:
            self.logger.info('Log location for this session: ' + logger_location)
            self.logger.info('Job/report processed by: ' + getpass.getuser() + ' on host: ' + socket.gethostname())
            self.logger.info(
                '=============================================================================================')
        print('\nDone!')


def run_from_cmd():
    XMLToJson().run()


if __name__ == '__main__':
    XMLToJson().run()
