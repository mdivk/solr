import os
import re
import bz2
import gzip
import json
from optparse import OptionParser

from lxml import etree


def parseXML(xmlfile):
    tree = etree.parse(xmlfile)
 
    root = tree.getroot()

    imexmlTradeNotificationMessageHeader = root.find('imexml:imexmlTradeNotificationMessageHeader', root.nsmap)
    imexmlMessageId = imexmlTradeNotificationMessageHeader.find('imexml:imexmlMessageId', root.nsmap)
    MESSAGE_ID = imexmlMessageId.text

    imexmlNotificationBundleDetails = root.find('imexml:imexmlNotificationBundleDetails', root.nsmap)
    imexmlPostEventTrades = imexmlNotificationBundleDetails.find('imexml:imexmlPostEventTrades', root.nsmap)


    for imexmlPostEventTrade in imexmlPostEventTrades:
        postEventTrade = imexmlPostEventTrade.find('imexml:postEventTrade', root.nsmap)


        tradeHeader = postEventTrade.find('fpml:tradeHeader', root.nsmap)
        for partyTradeIdentifier in tradeHeader:
            tradeId = partyTradeIdentifier.find('fpml:tradeId', root.nsmap)

            if tradeId is not None and tradeId.attrib['tradeIdScheme'] == 'http://www.dtcc.com/internal-reference-id':
                UITID = tradeId.text
            elif tradeId is not None and tradeId.attrib['tradeIdScheme'] == 'UniqueInternalTradeID':
                UITID = tradeId.text

        tradeDate = tradeHeader.find('fpml:tradeDate', root.nsmap)
        TRD_DATE = tradeDate.text

    return {
        'UITID': UITID,
        'MESSAGE_ID': MESSAGE_ID,
        'TRD_DATE': TRD_DATE
    }


def main():
    parser = OptionParser()
    parser.add_option("-r", "--region", dest="region",
                    help="enter region", metavar="PATH")
    parser.add_option("-f", "--flow", dest="flow",
                    help="enter flow", metavar="PATH")
    parser.add_option("-p", "--path", dest="bz2_archive",
                    help="zip file path", metavar="PATH")
    parser.add_option("-o", "--output", dest="json",
                    help="json file path", metavar="OUTPUT")
    parser.add_option("-q", "--quiet",
                    action="store_false", dest="verbose", default=True,
                    help="don't print status messages to stdout")

    (options, args) = parser.parse_args()

    try:
        region, flow = options.region, options.flow
    except TypeError:
        print('Param -p is empty')
        exit()
    except ValueError:
        region = flow = ''

    data = parseXML(options.bz2_archive)
    data['region'] = region
    data['flow'] = flow

    with open(options.json, 'w') as fp:
        json.dump(data, fp)

main()
