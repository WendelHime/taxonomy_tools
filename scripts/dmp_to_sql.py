"""Reads nodes.dmp and names.dmp and generate SQL files with the data"""
import os
import re
import sys
import logging
import argparse


def dmp_to_sql(line):
    """Reads a dmp line and returns a tuple of SQL fields
    Args:
        line: a string with the line to be converted
    Returns:
        A string with the line converted to SQL
    """
    script = ' ('
    regex = re.compile('([\da-zA-Z\s\-*<>.()"\',&\[\]\/:;_=@#\+\-\{\}?%`^!]+)+')
    matches = regex.findall(line)
    for value in matches:
        if re.match("\d+", value) is None:
            value = re.sub('\'', '\'\'', value)
            script += '\'{}\', '.format(value)
        else:
            script += '{}, '.format(value)
    script = re.sub('\t', '', script)
    script = re.sub(', \'\s+\',', '', script)
    script += ');\n'
    return script


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description="""Read taxonomy DB dump files and convert to SQL""")
    parser.add_argument('input_dir', type=str, help='Dir to taxdmp files')
    parser.add_argument(
            'output_dir',
            type=str,
            help='Output dir with sql files')
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
    input_dir = args.input_dir
    output_dir = args.output_dir
    logfile = args.logfile
    
    logging.basicConfig(
        filename=logfile,
        format='%(asctime)s : %(levelname)s : %(message)s',
        level=logging.INFO)

    logging.info('Reading %s file' % os.path.join(input_dir, 'nodes.dmp'))
    try:
        fh = open(os.path.join(input_dir, 'nodes.dmp'), 'r')
        lines = fh.readlines()
        fh.close()
    except Exception as ex:
        logging.error(
                'Error reading %s: %s' % (
                    os.path.join(input_dir, 'nodes.dmp'), str(ex)))
        sys.exit(1)

    logging.info('Checking existence of %s' % output_dir)
    if not os.path.exists(output_dir):
        try:
            logging.info('Directory not found, creating!')
            os.mkdir(output_dir)
        except Exception as ex:
            logging.error('Error creating output dir: %s' % str(ex))
            sys.exit(1)

    logging.info('Converting %s to %s' % (os.path.join(input_dir, 'nodes.dmp'),
        os.path.join(output_dir, 'nodes.sql')))
    fh = open(os.path.join(output_dir, 'nodes.sql'), 'a')
    table = 'CREATE TABLE nodes(\n'
    table += '\ttax_id INTEGER NOT NULL,\n'
    table += '\tparent_tax_id INTEGER,\n'
    table += '\trank VARCHAR(200)'
    table += ');\n'
    fh.write(table)
    fh.write('BEGIN TRANSACTION;\n')
    insert = 'INSERT INTO nodes(tax_id, parent_tax_id, rank) VALUES '
    for line in lines:
        fields = dmp_to_sql(line)
        regex = re.compile("(\((\d+), '(\d+)', '([\w\s\-_]+)')")
        matches = regex.findall(fields)
        fields = '({}, {}, \'{}\');'.format(
                matches[0][1],
                matches[0][2],
                matches[0][3])
        fh.write('{} {}\n'.format(insert, fields))
    fh.write('COMMIT;')
    fh.close()

    logging.info('Reading %s file' % os.path.join(input_dir, 'names.dmp'))
    try:
        fh = open(os.path.join(input_dir, 'names.dmp'), 'r')
        lines = fh.readlines()
        fh.close()
    except Exception as ex:
        logging.error(
                'Error reading %s: %s' % (
                    os.path.join(input_dir, 'names.dmp'), str(ex)))
        sys.exit(1)

    logging.info('Converting %s to %s' % (os.path.join(input_dir, 'names.dmp'),
        os.path.join(output_dir, 'names.sql')))
    fh = open(os.path.join(output_dir, 'names.sql'), 'a')
    table = 'CREATE TABLE names(\n'
    table += '\ttax_id INTEGER NOT NULL,\n'
    table += '\tname_txt VARCHAR(2000),\n'
    table += '\tunique_name VARCHAR(2000),\n'
    table += '\tname_class VARCHAR(2000)\n'
    table += ');'
    fh.write(table)
    fh.write('BEGIN TRANSACTION;\n')
    insert = 'INSERT INTO names(tax_id,name_txt,unique_name,name_class) VALUES '
    for line in lines:
        fields = dmp_to_sql(line)
        fh.write('{} {}\n'.format(insert, fields))
    fh.write('COMMIT;')
    fh.close()

    logging.info('Done')
