from optparse import OptionParser
import gzip
import json
import bz2
import os
import time
import uuid

from values import values, tags

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

target_bz2 = ""

try:
    region, flow = options.region, options.flow
except TypeError:
    print('Param -p is empty')
    exit()
except ValueError:
    region = flow = ''

names = dict(zip(tags, values))


def read_file(directory):
    flag_file_exist = False
    try:
        files = os.listdir(directory)
    except EnvironmentError as e:
        files = []
        print(str(e))
        exit()
    if not files:
        yield directory

    i = 0

    for each_file in files:
        global target_bz2
        target_bz2 = each_file
        try:
            # file_data = open_func('{}/{}'.format(directory, each_file), 'rt', encoding='utf-8', errors='ignore')
            # BZ2File(file, "w")
            file_data = bz2.BZ2File(os.path.join(directory, each_file), 'r')
        except EnvironmentError as e:
            file_data = []
            print('File Not Found'+str(e))
            exit()


        fix_row = file_data.read()
        i = i + 1;

        yield fix_row, each_file[:-4] + '.json'


# yield fix_row, '{}/{}'.format(res, each_file.split('.')[0])
# each_file[:-4]: 'apeq.qa_hk_rio_apares_to_users1.55.20180330.bz2'
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
                    new_dict_data['path'] = options.bz2_archive + "/" + target_bz2
                    new_dict_data['order_id'] = dict_data.get('OrderID')
                    new_dict_data['trade_date'] = dict_data.get('TradeDate')
                    new_dict_data['id'] = str(uuid.uuid4())

                    filename = options.json + '/%s_%s' % (each_data[1].rsplit(".", 1)[0], line) if len(d) > 1 else \
                    each_data[1].rsplit(".", 1)[0]
                    print('%s.json' % filename)
                    with open('%s.json' % filename, 'w') as json_data:
                        #print('%s.json' % filename)
                        json.dump(
                            new_dict_data,
                            json_data
                        )
            except EnvironmentError as e:
                print('File Not Found (record)'+str(e))


write_key_value(read_file('{p}'.format(p=options.bz2_archive)))

