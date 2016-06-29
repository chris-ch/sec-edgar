from datetime import datetime
import logging
import zipfile

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

    return callback(contents)


def load_reports_company(start_year, start_quarter, filing_type='10-Q', line_filter=None):
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

    def contents_processor(contents):
        for content in contents:
            for line in content.splitlines():
                fields = line.split()
                if len(fields) >= 4 and filing_type in fields[-4] and filter_ok(line) and line.strip() != '':
                    yield line

    return load_index(start_year, start_quarter, filename='company', callback=contents_processor)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.info('started')

    set_cache_ftp('../data/.ftpcache')
    reports = load_reports_company(2014, 1, line_filter='hallador', filing_type='10-Q')
    for report in reports:
        print(report)
