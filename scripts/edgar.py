import io
from datetime import datetime
import logging
import zipfile
import binascii

from urlcaching import download_ftp, set_cache_ftp

_SEC_URL = 'https://www.sec.gov'
_SEC_FTP = 'ftp.sec.gov'


def next_quarter(year, quarter):
    next_year = year
    next_quarter = quarter + 1
    if quarter > 3:
        next_quarter = 1
        next_year += 1

    return next_year, next_quarter


def range_quarters(start_year, start_quarter, end_year, end_quarter):
    year, quarter = start_year, start_quarter
    while year < end_year or (year == end_year and quarter <= end_quarter):
        yield year, quarter
        year, quarter = next_quarter(year, quarter)


def load_index(start_year, start_quarter, filename, callback=None):
    """

    :param start_year:
    :param start_quarter:
    :param filename:
    :param callback: index contents processor
    :return:
    """
    if callback is None:
        callback = lambda x: x

    today = datetime.today()
    end_year = today.year
    end_quarter = (today.month - 1) // 4 + 1
    quarters = range_quarters(start_year, start_quarter, end_year, end_quarter)
    contents = list()
    for year, quarter in quarters:
        filing_path = 'edgar/full-index/%s/QTR%s' % (year, quarter)
        content_io = download_ftp(_SEC_FTP, filing_path, '%s.zip' % filename)
        archive = zipfile.ZipFile(content_io, 'r')
        content = archive.read('%s.idx' % filename).decode('utf-8', 'backslashreplace')
        contents.append(content)

    for content in contents:
        lines = callback(content.splitlines())
        for line in lines:
            yield line


def load_index_xbrl(start_year, start_quarter, company_pattern, filing_type='10-Q'):

    def content_processor(lines):
        output_lines = list()
        start_marker = '--------------------'
        start_flag = False
        for line in lines:
            if not start_flag:
                if line.startswith(start_marker):
                    start_flag = True

                continue

            fields = line.split('|')
            if filing_type in fields[2] and company_pattern.upper() in fields[1].upper():
                doc_details = {
                    'CIK': fields[0],
                    'company': fields[1],
                    'filing': fields[2],
                    'date': fields[3],
                    'path': '/'.join(fields[4].split('/')[:-1]),
                    'filename': fields[4].split('/')[-1],
                }
                output_lines.append(doc_details)

        return output_lines

    return load_index(start_year, start_quarter, filename='xbrl', callback=content_processor)


def load_index_text(start_year, start_quarter, filing_type='10-Q', line_filter=None):
    """

    :param start_year:
    :param start_quarter:
    :param filing_type:
    :param line_filter:
    :return:
    """
    def filter_ok(line):
        if line_filter:
            return line_filter.upper() in line.upper()

        else:
            return True

    def content_processor(lines):
        output_lines = list()
        for line in lines:
            fields = line.split()
            if len(fields) >= 4 and filing_type in fields[-4] and filter_ok(line) and line.strip() != '':
                 output_lines.append(line)

        return output_lines

    return load_index(start_year, start_quarter, filename='company', callback=content_processor)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.info('started')

    set_cache_ftp('../data/.ftpcache')

    index = load_index_xbrl(2016, 1, 'hallador', filing_type='10-Q')
    for filing in index:
        print(filing)
        filename_txt = filing['filename']
        filename_base = filename_txt.split('.')[0]
        content = download_ftp('ftp.sec.gov', filing['path'], filename_txt).getvalue()
        lines = content.decode('utf-8', 'backslashreplace').splitlines()
        in_xbrl = False
        in_xbrl_body = False
        xbrl_zip_bytes = b''
        for line in lines:
            if line.startswith('<FILENAME>%s-xbrl.zip' % filename_base):
                in_xbrl = True
                continue
            elif in_xbrl:
                if line.startswith('begin '):
                    in_xbrl_body = True
                    continue

                elif len(line) == 0:
                    in_xbrl = False
                    continue

                elif in_xbrl_body:
                    xbrl_zip_bytes += binascii.a2b_uu(line)

        xbrl_zip = zipfile.ZipFile(io.BytesIO(xbrl_zip_bytes))
        for xbrl_file in xbrl_zip.infolist():
            print(xbrl_file)

    #reports = load_reports_company(2015, 1, line_filter='hallador', filing_type='10-Q')
    #for report in reports:
    #    print(report)
