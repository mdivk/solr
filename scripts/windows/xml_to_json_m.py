import os
import bz2
import json
from glob import glob
from lxml import etree
from optparse import OptionParser
from pathos.multiprocessing import ProcessingPool as Pool


class XMLToJson():
    def __init__(self, region=None, flow=None, path=None, output=None):
        self.process_options(region, flow, path, output)

    def process_options(self, region, flow, path, output):
        parser = OptionParser()
        parser.add_option("-r", "--region", dest="region", help="Enter region", metavar="PATH")
        parser.add_option("-f", "--flow", dest="flow",help="enter flow", metavar="PATH")
        parser.add_option("-p", "--path", dest="input_dir", help="bz2 file path", metavar="PATH")
        parser.add_option("-o", "--output", dest="json", help="json file path", metavar="OUTPUT")
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

        if not options.input_dir:
            if not path:
                parser.error('input directory not provided (-p option)')
            else:
                options.input_dir = path

        if not os.path.exists(options.input_dir):
            parser.error('input directory does not exist (-p option)')

        if not os.path.isdir(options.input_dir):
            parser.error('please enter a directory name (-p option)')

        if not options.json:
            if not output:
                parser.error('json output filename not provided (-o option)')
            else:
                options.json = output

        if not options.json.endswith('.json'):
            parser.error('please enter a json filename (-o option)')

        self.options = options


    def parseXML(self, xml_data):
        root = etree.fromstring(xml_data)

        data = {
            'UITID': '',
            'MESSAGE_ID': '',
            'TRD_DATE': ''
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
            if tradeDate is not None:
                data['TRD_DATE'] = tradeDate.text

            for partyTradeIdentifier in tradeHeader:
                tradeId = partyTradeIdentifier.find('fpml:tradeId', root.nsmap)

                if tradeId is not None and tradeId.attrib['tradeIdScheme'] == 'http://www.dtcc.com/internal-reference-id':
                    data['UITID'] = tradeId.text
                elif tradeId is not None and tradeId.attrib['tradeIdScheme'] == 'UniqueInternalTradeID':
                    data['UITID'] = tradeId.text

        return data


    def read_file(self, filename):
        try:
            file_data = bz2.BZ2File(filename, 'rb')
            return file_data.read()
        except:
            print('Error while reading ', filename)


    def process_file(self, f):
        xml_data = self.read_file(f)
        try:
            print('Processing: ' + f)
            d = self.parseXML(xml_data)
        except:
            print('Error while parsing the XML file', f)
            return {}

        d['REGION'] = self.options.region
        d['FLOW'] = self.options.flow
        return d


    def get_bz2_files(self):
        files_l = []
        for x in os.walk(self.options.input_dir):
            for y in glob(os.path.join(x[0], '*.bz2')):
                files_l.append(y)

        return files_l


    def run(self):
        pool = Pool(processes=1)
        self.json_data = pool.map(self.process_file, self.get_bz2_files())

        with open(self.options.json, 'w') as fp:
            json.dump(self.json_data, fp)


def run_from_cmd():
    XMLToJson().run()

if __name__ == '__main__':
    XMLToJson().run()
