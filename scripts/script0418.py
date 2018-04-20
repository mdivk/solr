from optparse import OptionParser
import gzip
import json
import bz2
import os
from values import values, tags
from pathlib import Path
import re

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

res = options.json
#res = "C:\\indexer_test\\test.json"

try:
    region, flow = options.region, options.flow
except TypeError:
    print('Param -p is empty')
    exit()
except ValueError:
    # print('Not enouth params')
    # exit()
    region = flow = ''

try:
    os.mkdir(options.json)
except FileExistsError:
    pass
except TypeError:
    print('Param -o is empty')
    exit()

names = dict(zip(tags, values))


def read_file(directory):
    flag_file_exist = False
    try:
        files = os.listdir(directory)
    except FileNotFoundError:
        files = []
        print('"{}" directory Not Found (search)'.format(directory))
        exit()
    if not files:
        yield directory

    i=0
    #open_func = bz2.open
    open_func = bz2.BZ2File
    file_data = []
    for each_file in files:
        suf = ''.join(Path(each_file).suffixes)
        if suf.endswith('.gz'):
            open_func = gzip.open
        elif suf.endswith('.bz2'):
            open_func = bz2.open
        else:
            try:
                os.mkdir(res)
            except FileExistsError:
                flag_file_exist = True
                pass

        if not flag_file_exist:
            try:
                #file_data = open_func('{}/{}'.format(directory, each_file), 'rt', encoding='utf-8', errors='ignore')
                file_data = open_func(os.path.join(directory, each_file), 'rt', encoding='utf-8', errors='ignore')
            except FileNotFoundError:
                file_data = []
                print('File Not Found')
                exit()

        fix_row = file_data.read()
        i=i+1;
        # #f = open('C:\\123\\abc.json', 'w')
        # f = open(res, 'w')
        # if isinstance(fix_row, bytes):
        #     fix_row = fix_row.decode('utf-8')
        # print(str(fix_row))
        # f.write(fix_row)
        # f.flush()
        # f.close()
        yield fix_row, each_file[:-4]+ '.json'
        #yield fix_row, '{}/{}'.format(res, each_file.split('.')[0])
        #each_file[:-4]: 'apeq.qa_hk_rio_apares_to_users1.55.20180330.bz2'
def write_key_value(data):
    data = list(data)
    for each_data in data:
        if each_data[0]:
            if isinstance(each_data, str):
                print('"{}" directory is empty'.format(data[0]))
                break
            try:
                d = each_data[0].strip().split('\n')
                for line, message in enumerate(d, start=1):
                    fix_split = message.strip().replace('\x02', '').split('\x01')
                    dict_data = dict(map(lambda items: (names.get(items.split('=')[0], items.split('=')[0]),
                                                        items.split('=')[1]),
                                         filter(None, fix_split)))
                    new_dict_data = dict()

                    new_dict_data['region'] = region
                    new_dict_data['flow'] = flow
                    new_dict_data['path'] = options.bz2_archive
                    new_dict_data['order_id'] = dict_data.get('OrderID')
                    new_dict_data['trade_date'] = dict_data.get('TradeDate')
                    new_dict_data['id'] = dict_data.get('OrderID') + "_" + dict_data.get('TradeDate')
                    # new_dict_data['order_id'] = dict_data.get('OrderID', '')
                    # new_dict_data['order_version'] = dict_data.get('10240', '')
                    # new_dict_data['trd_date'] = dict_data.get('TradeDate', '')
                    new_dict_data['message'] = ''.join(fix_split)
                    # new_list_data = [
                    #     {'region': new_dict_data['region']},
                    #     {'system': new_dict_data['system']},
                    #     {'order_id': new_dict_data['order_id']},
                    #     {'order_version': new_dict_data['order_version']},
                    #     {'trd_date': new_dict_data['trd_date']},
                    #     {'message': new_dict_data['message']}
                    # ]
                    new_dict_data.update(dict_data)

                    #filename = '%s%s' % (each_data[1], line) if len(d) > 1 else each_data[1]
                    #filename = '%s_%s' % (each_data[1].rsplit(".", 1)[0], line) if len(d) > 1 else each_data[1].rsplit(".", 1)[0]
                    filename = options.json + '\%s_%s' % (each_data[1].rsplit(".", 1)[0], line) if len(d) > 1 else each_data[1].rsplit(".", 1)[0]

                    with open('%s.json' % filename, 'w') as json_data:
                        json.dump(
                            # new_list_data,
                            new_dict_data,
                            json_data
                        )
            except FileNotFoundError:
                print('File Not Found (record)')

#read_file('{p}'.format(p=options.bz2_archive)): the folder of bz2 archives: -p C:\Users\RX52019\PycharmProjects\FIX_Indexer\test_data
write_key_value(read_file('{p}'.format(p=options.bz2_archive)))

#write_key_value(read_file("C:\indexer_test"))
