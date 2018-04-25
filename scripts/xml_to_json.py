import os
import re
import sys
import bz2
import json
from glob import glob
from lxml import etree
from optparse import OptionParser


class XMLToJson():
    def __init__(self, region=None, flow=None, path=None, output=None):
        self.json_data = []
        self.process_options(region, flow, path, output)

    def process_options(self, region, flow, path, output):
        parser = OptionParser()
        parser.add_option("-r", "--region", dest="region", help="Enter region", metavar="PATH")
        parser.add_option("-f", "--flow", dest="flow",help="enter flow", metavar="PATH")
        parser.add_option("-p", "--path", dest="input_dir", help="bz2 file path", metavar="PATH")
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

        if not options.input_dir:
            if not path:
                parser.error('input directory not provided (-p option)')
            else:
                options.input_dir = path

        if not os.path.exists(options.input_dir):
            parser.error('input directory does not exist (-p option)')

        if not os.path.isdir(options.input_dir):
            parser.error('please enter a directory name (-p option)')

        if not options.output_dir:
            if not output:
                parser.error('output folder not provided (-o option)')
            else:
                options.output_dir = output

        if not os.path.exists(options.output_dir) or not os.path.isdir(options.output_dir):
            os.makedirs(options.output_dir)

        self.options = options


    def sanitize(self, content):
        _illegal_unichrs = [(0x00, 0x08), (0x0D, 0x1F), 
        (0x7F, 0x84), (0x86, 0x9F), (0xFDD0, 0xFDDF), (0xFFFE, 0xFFFF)] 

        if sys.maxunicode >= 0x10000:  # not narrow build 
                _illegal_unichrs.extend([(0x1FFFE, 0x1FFFF), (0x2FFFE, 0x2FFFF), 
                                        (0x3FFFE, 0x3FFFF), (0x4FFFE, 0x4FFFF), 
                                        (0x5FFFE, 0x5FFFF), (0x6FFFE, 0x6FFFF), 
                                        (0x7FFFE, 0x7FFFF), (0x8FFFE, 0x8FFFF), 
                                        (0x9FFFE, 0x9FFFF), (0xAFFFE, 0xAFFFF), 
                                        (0xBFFFE, 0xBFFFF), (0xCFFFE, 0xCFFFF), 
                                        (0xDFFFE, 0xDFFFF), (0xEFFFE, 0xEFFFF), 
                                        (0xFFFFE, 0xFFFFF), (0x10FFFE, 0x10FFFF)])

        _illegal_ranges = ["%s-%s" % (chr(low), chr(high)) 
                        for (low, high) in _illegal_unichrs] 
        _illegal_xml_chars_RE = re.compile(u'[%s]' % u''.join(_illegal_ranges))
        
        content = re.sub(_illegal_xml_chars_RE, "", content)
        content = re.sub(r"[\x0A-\x0C]", "\n", content)
        return content


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
        file_data = self.read_file(f).replace('\x02', '')
        if file_data.count('<?xml version="1.0" encoding="UTF-8"?>') <= 1:
            try:
                d = self.parseXML(file_data)
                d['REGION'] = self.options.region
                d['FLOW'] = self.options.flow
                d['PATH'] = f
            except:
                print('Error while parsing the XML file', f)
                return {}
        else:
            d = []
            for xml_data in file_data.split('<?xml version="1.0" encoding="UTF-8"?>'):
                if not xml_data:
                    continue
                try:
                    t = self.parseXML(xml_data)
                    t['REGION'] = self.options.region
                    t['FLOW'] = self.options.flow
                    t['PATH'] = f
                    d.append(t)
                except:
                    pass
        return d


    def get_bz2_files(self):
        files_l = []
        for x in os.walk(self.options.input_dir):
            for y in glob(os.path.join(x[0], '*.bz2')):
                files_l.append(y)

        return files_l


    def run(self):
        for f in self.get_bz2_files():

            output_f = self.options.output_dir + os.sep + f

            d = self.process_file(f)

            if not d:
                continue

            if not os.path.exists(os.path.dirname(output_f)):
                try:
                    os.makedirs(os.path.dirname(output_f))
                except OSError as exc: # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

            if isinstance(d, (list,)):
                for i, data in enumerate(d):
                    with open(output_f + '_' + str(i + 1) + '.json', 'w') as fp:
                        json.dump(data, fp)
            else:
                with open(output_f + '.json', 'w') as fp:
                    json.dump(d, fp)


def run_from_cmd():
    XMLToJson().run()

if __name__ == '__main__':
    XMLToJson().run()
