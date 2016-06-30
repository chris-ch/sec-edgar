import logging
import shelve
from ftplib import FTP

import requests
import requests_cache
from io import BytesIO

_cache_file_path = None


def set_cache_http(cache_file_path):
    requests_cache.install_cache(cache_file_path)


def open_url(url):
    return requests.get(url).text


def set_cache_ftp(cache_file_path):
    global _cache_file_path
    _cache_file_path = cache_file_path


def ftp_retrieve(server, path, filename):
    logging.info('loading: ftp://%s/%s/%s' % (server, path, filename))
    ftp = FTP(server)
    ftp.login()
    ftp.cwd(path)
    buffer = BytesIO()
    ftp.retrbinary('RETR %s' % filename, buffer.write)
    return buffer


def download_ftp(server, path, filename, refresh_cache=False):
    if _cache_file_path:
        url_cache = shelve.open(_cache_file_path)
        location = '/'.join([server, path, filename])
        if location not in url_cache or refresh_cache:
            url_cache[location] = ftp_retrieve(server, path, filename)

        output = url_cache[location]
        url_cache.close()

    else:
        output = ftp_retrieve(server, path, filename)

    return output
