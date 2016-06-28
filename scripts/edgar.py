import logging

from bs4 import BeautifulSoup

from urlcaching import open_url, set_cache_file

_SEC_URL = 'https://www.sec.gov'

_EDGAR_URL = _SEC_URL + '/cgi-bin/browse-edgar'


def find_tag_content(soup, *args, **kwargs):
    tag = soup.find(*args, **kwargs)
    if tag.contents:
        return tag.contents[0]

    else:
        return None


def find_tag_contents(soup, *args, **kwargs):
    tag = soup.find(*args, **kwargs)
    if tag.contents:
        return tag.contents

    else:
        return list()


def fast_search(ticker):
    url = _EDGAR_URL + '?' + 'CIK=%s&owner=exclude&action=getcompany&Find=Search'
    html_text = open_url(url % ticker)
    html = BeautifulSoup(html_text, 'html.parser')
    company_name_tag = find_tag_contents(html, 'span', {'class': 'companyName'})
    company_name, company_cik = company_name_tag[0], company_name_tag[3].contents[0].split()[0]
    return company_name.strip(), company_cik.strip()


def load_filing_details(filing_location):
    url = _SEC_URL + filing_location
    filing_details_text = open_url(url)
    try:
        filing_details_html = BeautifulSoup(filing_details_text, 'html.parser')
        interactive_url_marker = filing_details_html.find('div', {'id': 'seriesDiv'})
        interactive_url = ''
        if interactive_url_marker:
            interactive_url = interactive_url_marker.find('a')['href']

        form_groupings = filing_details_html.find_all('div', {'class': 'formGrouping'})
        rows = list()
        for form_grouping in form_groupings:
            rows += form_grouping.find_all('div')

        filing_details = dict()
        head = ''
        for row in rows:
            if row['class'][0] == 'infoHead':
                head = row.contents[0]

            else:
                filing_details[head] = row.contents[0]

        doc_files = list()
        doc_format_files_table = filing_details_html.find_all('table', {'summary': 'Document Format Files'})
        if doc_format_files_table:
            for row in doc_format_files_table[0].find_all('tr'):
                columns = list()
                for column in row.find_all('td'):
                    columns.append(column.contents[0])

                if columns:
                    doc_files.append((columns[1], columns[2]['href'], columns[3]))

        data_files = list()
        data_files_table = filing_details_html.find_all('table', {'summary': 'Data Files'})
        if data_files_table:
            for row in data_files_table[0].find_all('tr'):
                columns = list()
                for column in row.find_all('td'):
                    columns.append(column.contents[0])

                if columns:
                    data_files.append((columns[1], columns[2]['href'], columns[3]))

        filing_details['doc_files'] = doc_files
        filing_details['data_files'] = data_files
        filing_details['interactive_url'] = interactive_url

    except Exception as err:
        logging.error('an error occured while downloading url "%s"', url)
        raise

    return filing_details


def load_filings_pointers(cik, filing_type='10-Q', filings_count=200):
    url = _EDGAR_URL + '?' + 'action=getcompany&CIK=%s&type=%s&dateb=&owner=exclude&count=%s'
    html_text = open_url(url % (cik, filing_type, filings_count))
    html = BeautifulSoup(html_text, 'html.parser')
    filings = list()
    table = html.find('table', {'class': 'tableFile2'})
    for count, row in enumerate(table.find_all('tr')):
        if count == 0:
            pass

        else:
            filing = list()
            for column, field in enumerate(row.find_all('td')):
                if column == 1:
                    filing.append(str(field.contents[0]['href']))

                elif column == 4:
                    pass

                else:
                    filing.append(str(field.contents[0]))

            filings.append(filing)

    for filing in filings:
        if filing[0] == filing_type:
            filing_details = load_filing_details(filing[1])
            filing.append(filing_details)

    return filings


def main():
    set_cache_file('../data/.urlcache')
    name, cik = fast_search('CYH')
    print('"%s","%s"' % (name, cik))
    filings = load_filings_pointers(cik, filing_type='10-Q', filings_count=200)
    for filing in filings:
        if filing[0] == '10-Q' and filing[4]['interactive_url']:
            print(filing[4]['interactive_url'], filing[4]['Period of Report'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.info('started')

    #load_filing_details('/Archives/edgar/data/1108109/000119312516575053/0001193125-16-575053-index.htm')
    main()
