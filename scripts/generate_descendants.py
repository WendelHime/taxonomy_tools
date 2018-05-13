"""Reads a dmp file and produces an SQL output"""
import os
import re
import sys
import sqlite3
import argparse
import logging


def read_dmp(line):
    """Reads a .dmp line and produces a list of values
    Args:
        line: a string to be converted
    Returns:
        A list with matched values
    """
    regex = re.compile('([\da-zA-Z\s\-*<>.()"\',&\[\]\/:;_=@#\+\-\{\}?%`^!]+)+')
    matches = regex.findall(line)
    clean_list = []
    for match in matches:
        match = match.replace('\t', '')
        match = match.replace('\n', '')
        clean_list.append(match)
    return clean_list


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description="""Reads a dmp file and generates a CSV""")
    parser.add_argument('input_dir', type=str, help='Dir to taxdmp files')
    parser.add_argument('output_dir', type=str, help='Dir for storage results')
    parser.add_argument(
            'rank_id',
            type=str,
            help='Rank ID, example of viruses: 10239')
    parser.add_argument('--logfile', type=str, help='Logfile')
    parser.add_argument(
            '-v',
            '--version',
            action='version',
            version='%(prog)s 0.1')
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()
    filedir = args.input_dir
    output = args.output_dir
    rank_id = args.rank_id
    logfile = args.logfile

    logging.basicConfig(
        filename=logfile,
        format='%(asctime)s : %(levelname)s : %(message)s',
        level=logging.INFO)

    logging.info('Reading %s file' % os.path.join(filedir, 'nodes.dmp'))
    try:
        fh = open(os.path.join(filedir, 'nodes.dmp'), "r")
        lines = fh.readlines()
        fh.close()
    except Exception as ex:
        logging.error(
                'Error reading %s: %s' % (
                    os.path.join(filedir, 'nodes.dmp'), str(ex)))
        sys.exit(1)

    logging.info('Checking existence of %s' % output)
    if not os.path.exists(output):
        try:
            logging.info('Directory not found, creating!')
            os.mkdir(output)
        except Exception as ex:
            logging.error('Error creating output dir: %s' % str(ex))
            sys.exit(1)
    fh_nodes = open(os.path.join(output, 'nodes.csv'), 'a')
    
    logging.info('Getting nodes descendants from: %s' % str(rank_id))
    green_ids = [rank_id]
    while green_ids:
        green_id = green_ids.pop(0)
        for line in lines:
            register = read_dmp(line)
            if register[1] == green_id:
                green_ids.append(register[0])
                fh_nodes.write(','.join(register) + '\n')

    fh_nodes.close()
    logging.info('Done!')
