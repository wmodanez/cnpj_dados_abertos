
import os
import time
import urllib
import datetime
import requests
import progressbar
from bs4 import BeautifulSoup

URL: str = 'http://200.152.38.155/CNPJ/'
ZIP_FOLDER: str = 'dados-publicos-zip/'

progress_bar: progressbar = None


def show_progress(block_num: int, block_size: int, total_size: int):
    global progress_bar

    if progress_bar is None:
        progress_bar = progressbar.ProgressBar(maxval=total_size)
        progress_bar.start()

    downloaded: int = block_num * block_size

    if downloaded < total_size:
        progress_bar.update(downloaded)
    else:
        progress_bar.finish()
        progress_bar = None


def check_file_exists(path: str, filename: str) -> int:
    if os.path.exists(path + filename):
        return os.stat(path + filename).st_size
    return 0


soup: BeautifulSoup = BeautifulSoup(requests.get(URL).text)


def download_file():
    urllib.request.urlretrieve(file_url, ZIP_FOLDER + file_download, show_progress)
    os.utime(ZIP_FOLDER + file_download, (timestamp_last_modified, timestamp_last_modified))


for link in soup.find_all('a'):
    if str(link.get('href')).endswith('.zip'):
        file_download: str = link.get('href')
        file_url: str = URL + file_download

        if not file_download.startswith('http'):
            local_file: int = check_file_exists(ZIP_FOLDER, file_download)
            # print(requests.head(url_file).headers)
            file_url_last_modified: list = requests.head(file_url).headers['Last-Modified'].split()

            file_url_last_modified_time: str = str(file_url_last_modified[4]).split(':')
            timestamp_last_modified: int = datetime.datetime(int(file_url_last_modified[3]),
                                                             int(time.strptime(file_url_last_modified[2], '%b').tm_mon),
                                                             int(file_url_last_modified[1]),
                                                             int(file_url_last_modified_time[0]),
                                                             int(file_url_last_modified_time[1]),
                                                             int(file_url_last_modified_time[2])).timestamp()

            print('Baixando o arquivo: ' + file_download)
            if local_file == 0:
                download_file()
            elif local_file > 0 and local_file != int(requests.head(file_url).headers['Content-Length']):
                download_file()
            else:
                print('O arquivo', file_download, 'esta atualizado.')
        else:
            print('Não foi possível baixar o arquivo: ' + file_download)

print('Fim!')
