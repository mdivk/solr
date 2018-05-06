import os
import bz2
import sys
import json
import errno
import urllib2

from optparse import OptionParser

POST_URL = '<URL FROM WHERE THE PATH CAN BE READ>'


def read_bz2_file(filename):
    try:
        file_data = bz2.BZ2File(filename, 'rb')
        return file_data.read()
    except:
        print('Error while reading ' + filename)


def create_path_if_not_exists(p):
    if not os.path.exists(os.path.dirname(p)):
        try:
            print('Creating path ' + os.path.dirname(p))
            os.makedirs(os.path.dirname(p))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise


parser = OptionParser()
parser.add_option("-p", "--path", dest="input_path", help="bz2 file path", metavar="PATH")
parser.add_option("-o", "--output", dest="output_path", help="output file path", metavar="OUTPUT")
(options, args) = parser.parse_args()


input_paths = []

try:
    # uncomment these lines when the POST_URL is ready
    # response = urllib2.urlopen(POST_URL)
    # contents = json.load(response)
    #Delete the next line when the POST_URL is ready
    contents = json.loads('''{"responseHeader":{"status":0,"QTime":0,"params":{"q":"order_id:180937121tj2","indent":"true","wt":"json"}},"response":{"numFound":3,"start":0,"docs":[{"order_id":"180937121tj2","region":"apac","flow":"apeq","trade_date":"20180403","path":"/tmp/solr_data/apac/apeq/odac/2018/04/20180403/data/apeq.qa_hk_rio_apff_to_users1.24.20180403.bz2","id":"d9ff342e-e472-46a0-83f2-9dad9bc5b497","_version_":"1598834492450013184"},{"order_id":"180937121tj2","region":"apac","flow":"apeq","trade_date":"20180403","path":"/tmp/solr_data/apac/apeq/odac/2018/04/20180403/data/apeq.qa_hk_rio_apff_to_users1.24.20180403.bz2","id":"d09688a0-4caf-4286-aa01-244c1704ec05","_version_":"1598834793756229632"},{"order_id":"180937121tj2","region":"apac","flow":"apeq","trade_date":"20180403","path":"/tmp/solr_data/apac/apeq/odac/2018/04/20180403/data/apeq.qa_hk_rio_apff_to_users1.24.20180403.bz2","id":"99211ee0-01c2-4fde-b14e-275451f4bdec","_version_":"1598835235186802688"}]}}''')
    for doc in contents['response']['docs']:
        if doc['path'] not in input_paths:
            input_paths.append(doc['path'])
except:
  print('Error reading the PATH from POST_URL')
  sys.exit(-1)

print_to_console = False

if not options.output_path:
    print_to_console = True
else:
    options.output_path = options.output_path.rstrip('/').rstrip('\\')


for input_path in input_paths:
    print('Processing file ' + input_path)
    output_fpath = os.path.join(options.output_path, os.path.basename(input_path).rstrip('.bz2'))
    create_path_if_not_exists(output_fpath)

    data = read_bz2_file(input_path)

    if print_to_console:
        print(data)
    else:
        print('Saving file ' + output_fpath)
        with open(output_fpath, 'w') as fp:
	        fp.write(data)