import bz2
import datetime
import errno
import hashlib
import json
import logging
import os
import getpass
import socket
import re
import sys
from glob import glob
from optparse import OptionParser

from lxml import etree

from values import values, tags

names = dict(zip(tags, values))
target_bz2 = ""
output_f = ""

class XMLToJson():
    def __init__(self, msgtype=None, region=None, flow=None, path=None, output=None):
        '''
        Create the XMLToJson Object
        constructor parameters can be omitted if the commandline parameters are passed
        Usage: python2.7 xml_to_json.py -r xregion -f xflow -p input_path -o output_dir
        -p input_path can be a directory or an individual .bz2 file
        -o output_dir is optional. default value is 'json'
        '''

        self.json_data = []
        self.process_options(msgtype, region, flow, path, output)
        output_f = os.path.join(self.options.output_dir, self.options.input_path.strip('/'))
        self.create_log_directory()
        self.config_logger()
        self.create_path_if_not_exists(output_f)
        self.logger_location = ''

    def process_options(self, msgtype, region, flow, path, output):
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
        parser.add_option("-o", "--output", dest="output_dir", help="json file path", metavar="OUTPUT")
        parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True,
                          help="don't print status messages to stdout")

        (options, args) = parser.parse_args()

        if not options.msgtype:
            parser.error('msgtype not provided (-t option)')
        else:
            if options.msgtype != "fix" and options.msgtype != "xml":
                self.logger.error('Error: msgtype ' + str(
                    msgtype) + ', msgtype can be only fix or xml at the moment, please correct.')
                parser.error('msgtype can be only fix or xml at the moment, please correct.')
                exit(2)

        if not options.region:
            if not region:
                parser.error('region not provided (-r option)')
            else:
                options.region = region

        if not options.flow:
            if not flow:
                parser.error('flow value not provided (-f option)')
            else:
                options.flow = flow

        if not options.input_path:
            if not path:
                parser.error('input path not provided (-p option)')
            else:
                options.input_path = path

        if not os.path.exists(options.input_path):
            parser.error('input path does not exist (-p option)')

        options.input_path = options.input_path.rstrip('/').rstrip('\\')

        if not options.output_dir:
            options.output_dir = 'json'

        if not os.path.exists(options.output_dir) or not os.path.isdir(options.output_dir):
            os.makedirs(options.output_dir)

        self.options = options

        if os.path.isfile(options.input_path):
            self.options.is_file = True
        else:
            self.options.is_file = False


    def config_logger(self):
        global logger_location
        logger_location = os.getcwd() + '//log//' + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '.log'
        logging.basicConfig(filename=logger_location,
                            format='%(asctime)s %(levelname)s %(message)s')
        self.logger = logging.getLogger('xml_to_json_log')
        self.logger.setLevel(logging.DEBUG)

    def create_path_if_not_exists(self, p):
        '''
        If the path doesn't exist, create the path (all directories in the given path)
        '''
        if not os.path.exists(os.path.dirname(p)):
            try:
                self.logger.info('Creating path ' + os.path.dirname(p))
                os.makedirs(os.path.dirname(p))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

    def create_log_directory(self):
        if not os.path.exists(os.getcwd() + '\\log'):
            try:
                os.makedirs(os.getcwd() + '\\log')
            except OSError as ex:
                pass


    def save_json_file(self, f, data):
        with open(f, 'w') as fp:
            json.dump(data, fp)
        self.logger.info('Saved ' + f)

    def save_data(self, json_data, output_f):
        if not json_data:
            return

        if isinstance(json_data, (list,)):
            json_file_name = ''

            for i, each_json_data in enumerate(json_data):
                json_file_name = output_f + '_' + str(i + 1) + '.json'
                self.save_json_file(json_file_name, each_json_data)
            return i + 1
        else:
            self.save_json_file(output_f + '.json', json_data)
            return 1

    def parseXML(self, xml_data, fname):
        '''
        Parse the given XML data, extract the required info and
        return a dict object with some hardcod-ed custom values
        '''
        root = etree.fromstring(xml_data)

        data = {
            'ID': '',
            'UITID': '',
            'TRD_DATE': '',
            'MESSAGE_ID': '',
            'REGION': self.options.region,
            'FLOW': self.options.flow,
            'PATH': fname
        }

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

        data = {
            'id': '',
            'region': '',
            'flow': '',
            'path': '',
            'order_id': '',
            'trade_date': ''
        }

        data['id'] = hashlib.sha1(fix_data).hexdigest()

        fix_split = fix_data.strip().replace('\x02', '').split('\x01')
        dict_data = dict(map(lambda items: (names.get(items.split('=')[0], items.split('=')[0]),
                                            items.split('=')[1]),
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
                return self.parseFIX(file_data, f)
            except:
                self.logger.error('Error while parsing the XML file' + f)
                return {}
        else:
            d = []
            for fix_data in file_data.split(delimiter1):
                if not fix_data:
                    continue
                try:
                    d.append(self.parseFIX(delimiter1 + fix_data, f))
                except:
                    pass

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

    def run(self):
        start_time = datetime.datetime.now()
        json_file_cnt = 0
        # if self.options.input_is_file:
        #
        #     total_files = 1
        #     idx = 1
        #     total_skipped = 0
        #
        #     self.logger.debug('Output file path derived by joining Output directory: '
        #                       + self.options.output_dir + ' and ' + self.options.input_path + ' -'
        #                       + self.options.output_dir)
        #
        #     try:
        #         json_data = self.process_file(self.options.msgtype, self.options.input_path)
        #         json_file_cnt = self.save_data(json_data, self.options.output_dir)
        #     except Exception as ex:
        #         total_skipped += 1
        #         self.logger.error('Error: ' + str(ex))
        #
        #     stop_time = datetime.datetime.now()
        #     elapsed_time = stop_time - start_time
        #
        #     self.logger.info(
        #         '=============================================================================================')
        #     self.logger.info('Report for the session')
        #     self.logger.info('Time the session started: ' + str(start_time))
        #     self.logger.info('Time the session ended: ' + str(stop_time))
        #     self.logger.info('Time the session costs: ' + str(elapsed_time))
        #     self.logger.info('Data Type processed in this session: ' + self.options.msgtype)
        #     self.logger.info('Data Region processed in this session: ' + self.options.region)
        #     self.logger.info('Data Flow processed in this session: ' + self.options.flow)
        #     self.logger.info('Data processed in this session: ' + self.options.input_path)
        #     self.logger.info('Result generated in this session: ' + self.options.output_dir)
        #     self.logger.info('Total bz2 archives to be processed: ' + str(total_files))
        #     self.logger.info('Total bz2 archives processed: ' + str(idx))
        #     self.logger.info('Total bz2 archives failed: ' + str(total_skipped))
        #     self.logger.info('Total completed json outputs: ' + str(json_file_cnt))
        #     self.logger.info('Log location for this session: ' + logger_location)
        #     self.logger.info('Job/report processed by: ' + getpass.getuser() + ' on host: ' + socket.gethostname())
        #     self.logger.info(
        #         '=============================================================================================')
        #     print('\nDone!')
        #
        #     return

        total_files = str(self.get_bz2_files_count())

        idx = 0
        total_skipped = 0

        for idx, input_f in enumerate(self.get_bz2_files()):
            sys.stdout.write("\rProcessing file " + str(idx + 1) + "/" + total_files + '...')
            sys.stdout.flush()
            output_f = os.path.join(self.options.output_dir,
                                    input_f.replace(self.options.input_path, '').strip('\\').strip('/'))

            self.logger.debug(
                'Output file path derived by joining Output directory: ' + self.options.output_dir + ' and, ')
            self.logger.debug('input bz2 filename - ' + input_f.replace(os.path.dirname(self.options.input_path), '')
                              + ' by removing the  parent folderpath -' + os.path.dirname(self.options.input_path)
                              + ' from input bz2 file path - ' + input_f)

            try:
                self.create_path_if_not_exists(output_f)
                json_data = self.process_file(self.options.msgtype, input_f)

                #if input is single file, the file's name is to be used as part of the json file's name
                if self.options.is_file:
                    output_f = output_f + '\\' + os.path.basename(self.options.input_path)

                json_file_cnt += self.save_data(json_data, output_f)

                idx += 1
            except Exception as ex:
                total_skipped += 1
                self.logger.error('Error while parsing the file ' + input_f + ' Details: '+ str(ex))
                pass

        stop_time = datetime.datetime.now()
        elapsed_time = stop_time - start_time

        self.logger.info('=============================================================================================')
        self.logger.info('Report for the session')
        self.logger.info('Time the session started: ' + str(start_time))
        self.logger.info('Time the session ended: ' + str(stop_time))
        self.logger.info('Time the session costs: ' + str(elapsed_time))
        self.logger.info('Data Type processed in this session: ' + self.options.msgtype)
        self.logger.info('Data Region processed in this session: ' + self.options.region)
        self.logger.info('Data Flow processed in this session: ' + self.options.flow)
        self.logger.info('Data processed in this session: ' + self.options.input_path)
        self.logger.info('Result generated in this session: ' + self.options.output_dir)
        self.logger.info('Total bz2 archives to be processed: ' + str(total_files))
        self.logger.info('Total bz2 archives processed: ' + str(idx))
        self.logger.info('Total bz2 archives failed: ' + str(total_skipped))
        self.logger.info('Total completed json outputs: ' + str(json_file_cnt))
        self.logger.info('Log location for this session: ' + logger_location)
        self.logger.info('Job/report processed by: ' + getpass.getuser() + ' on host: ' + socket.gethostname())
        self.logger.info('=============================================================================================')
        print('\nDone!')


def run_from_cmd():
    XMLToJson().run()


if __name__ == '__main__':
    XMLToJson().run()
