import os
import re
import sys
import bz2
import json
import uuid
import errno
import logging
from glob import glob
from lxml import etree
from optparse import OptionParser



class XMLToJson():

    def __init__(self, region=None, flow=None, path=None, output=None):
        '''
        Create the XMLToJson Object
        constructor parameters can be omitted if the commandline parameters are passed
        Usage: python2.7 xml_to_json.py -r xregion -f xflow -p input_path -o output_dir
        -p input_path can be a directory or an individual .bz2 file
        -o output_dir is optional. default value is 'json'
        '''

        self.json_data = []
        self.process_options(region, flow, path, output)
        self.config_logger()


    def process_options(self, region, flow, path, output):
        '''
        Process the Options that the user provided
        either via commandline or
        as constructor parameters.
        '''

        parser = OptionParser()
        parser.add_option("-r", "--region", dest="region", help="Enter region", metavar="PATH")
        parser.add_option("-f", "--flow", dest="flow",help="enter flow", metavar="PATH")
        parser.add_option("-p", "--path", dest="input_path", help="bz2 file path", metavar="PATH")
        parser.add_option("-o", "--output", dest="output_dir", help="json file path", metavar="OUTPUT")
        parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True,
                            help="don't print status messages to stdout")

        (options, args) = parser.parse_args()

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
            self.options.input_is_file = True
        else:
            self.options.input_is_file = False


    def config_logger(self):
        logging.basicConfig(filename = 'log.txt', format = '%(asctime)s %(levelname)s %(message)s')
        self.logger = logging.getLogger('xml_to_json_log')
        self.logger.setLevel(logging.INFO)


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


    def save_json_file(self, f, data):
        with open(f, 'w') as fp:
            json.dump(data, fp)
        self.logger.info('Saved ' + f)


    def save_data(self, json_data, output_f):
            if not json_data:
                return

            self.create_path_if_not_exists(output_f)

            if isinstance(json_data, (list,)):
                for i, each_json_data in enumerate(json_data):
                    self.save_json_file(output_f + '_' + str(i + 1) + '.json', each_json_data)
            else:
                self.save_json_file(output_f + '.json', json_data)


    def parseXML(self, xml_data, fname):
        '''
        Parse the given XML data, extract the required info and
        return a dict object with some hardcod-ed custom values
        '''
        root = etree.fromstring(xml_data)

        data = {
            'ID': str(uuid.uuid4()),
            'UITID': '',
            'TRD_DATE': '',
            'MESSAGE_ID': '',
            'REGION': self.options.region,
            'FLOW': self.options.flow,
            'PATH': fname
        }

        imexmlTradeNotificationMessageHeader = root.find('imexml:imexmlTradeNotificationMessageHeader', root.nsmap)
        imexmlMessageId = imexmlTradeNotificationMessageHeader.find('imexml:imexmlMessageId', root.nsmap)
        if imexmlMessageId is not None:
            data['MESSAGE_ID'] = imexmlMessageId.text

        imexmlNotificationBundleDetails = root.find('imexml:imexmlNotificationBundleDetails', root.nsmap)
        imexmlPostEventTrades = imexmlNotificationBundleDetails.find('imexml:imexmlPostEventTrades', root.nsmap)

        for imexmlPostEventTrade in imexmlPostEventTrades:
            postEventTrade = imexmlPostEventTrade.find('imexml:postEventTrade', root.nsmap)

            tradeHeader = postEventTrade.find('fpml:tradeHeader', root.nsmap)
            tradeDate = tradeHeader.find('fpml:tradeDate', root.nsmap)
            if tradeDate is not None and data['TRD_DATE'] == '':
                data['TRD_DATE'] = tradeDate.text

            for partyTradeIdentifier in tradeHeader:
                tradeIds = partyTradeIdentifier.findall('.//fpml:tradeId', root.nsmap)

                for tradeId in tradeIds:
                    if tradeId is not None and tradeId.attrib['tradeIdScheme'] == 'http://www.dtcc.com/internal-reference-id' and data['UITID'] == '':
                        data['UITID'] = tradeId.text
                    elif tradeId is not None and tradeId.attrib['tradeIdScheme'] == 'UniqueInternalTradeID' and data['UITID'] == '':
                        data['UITID'] = tradeId.text
        return data


    def read_file(self, filename):
        try:
            file_data = bz2.BZ2File(filename, 'rb')
            return file_data.read()
        except:
            self.logger.error('Error while reading ' + filename)


    def process_file(self, f):
        '''
        Process a bz2 file
        returns either a dict object or a list of dict objects
        list of dict objects when there are 'multiple' xml file contents in the specified file.
        '''
        self.logger.info('Processing ' + f)
        file_data = self.read_file(f).replace('\x02', '')
        file_data = re.sub(r'<imexml:imexmlTradeNotification\s+xmlns', '<imexml:imexmlTradeNotification xmlns', file_data)

        if file_data.count('<?xml version="1.0" encoding="UTF-8"?>') == 0 and file_data.count('<imexml:imexmlTradeNotification xmlns') > 1:
            file_data = re.sub(r'<imexml:imexmlTradeNotification xmlns', '<?xml version="1.0" encoding="UTF-8"?><imexml:imexmlTradeNotification xmlns', file_data)

        if file_data.count('<?xml version="1.0" encoding="UTF-8"?>') <= 1:
            try:
                return self.parseXML(file_data, f)
            except:
                self.logger.error('Error while parsing the XML file' + f)
                return {}
        else:
            d = []
            for xml_data in file_data.split('<?xml version="1.0" encoding="UTF-8"?>'):
                if not xml_data:
                    continue
                try:
                    d.append(self.parseXML(xml_data, f))
                except:
                    pass
        return d


    def get_bz2_files_count(self):
        '''
        Get the count of bz2 files in options.input_path
        '''
        count = 0

        for x in os.walk(self.options.input_path):
            for y in glob(os.path.join(x[0], '*.bz2')):
                count += 1

        return count


    def get_bz2_files(self):
        '''
        Get list of bz2 files in options.input_path
        '''
        files_l = []

        for x in os.walk(self.options.input_path):
            for y in glob(os.path.join(x[0], '*.bz2')):
                files_l.append(y)

        return files_l


    def run(self):
        if self.options.input_is_file:
            output_f = os.path.join(self.options.output_dir, self.options.input_path)
            json_data = self.process_file(self.options.input_path)
            self.save_data(json_data, output_f)
            return

        total_files = str(self.get_bz2_files_count())

        for i, input_f in enumerate(self.get_bz2_files()):
            sys.stdout.write("\rProcessing file " + str(i + 1) + "/" + total_files + '...')
            sys.stdout.flush()
            output_f = os.path.join(self.options.output_dir, input_f.replace(os.path.dirname(self.options.input_path), ''))
            json_data = self.process_file(input_f)
            self.save_data(json_data, output_f)

        print('\nDone!')



def run_from_cmd():
    XMLToJson().run()


if __name__ == '__main__':
    XMLToJson().run()
