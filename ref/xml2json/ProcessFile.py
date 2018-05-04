import os
import bz2
import sys
from optparse import OptionParser


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

if not options.input_path:
	print('No input file path provided')
	print('-p <input file path> -o <output file path> [optional]')
	sys.exit(-1)

options.input_path = options.input_path.rstrip('/').rstrip('\\')

if not options.input_path.endswith('.bz2'):
	print('Input file not a .bz2 file')
	sys.exit(-1)

if not options.output_path:
    options.output_path = os.path.join(os.getcwd(), os.path.basename(options.input_path).rstrip('.bz2'))

options.output_path = options.output_path.rstrip('/').rstrip('\\')

if not os.path.isfile(options.output_path) and os.path.isdir(options.output_path):
    options.output_path = os.path.join(options.output_path, os.path.basename(options.input_path).rstrip('.bz2'))

create_path_if_not_exists(options.output_path)

data = read_bz2_file(options.input_path)

print('Saving file data in ' + options.output_path)
with open(options.output_path, 'w') as fp:
	fp.write(data)