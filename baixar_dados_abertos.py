import os
import time
import urllib
import logging
import datetime
import requests
import progressbar
from bs4 import BeautifulSoup

URL: str = 'http://200.152.38.155/CNPJ/'
ZIP_FOLDER: str = 'dados-abertos-zip/'
FILE_DOWNLOAD = os.path.join('dados-abertos-zip', 'downloads.txt')

progress_bar: progressbar = None

# logging.basicConfig(level=logging.INFO, filename='cnpj.log', filemode='a',
#                     format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')


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


def check_if_update_base(datetime_last_upload: datetime):
    last_download: str
    with open(FILE_DOWNLOAD) as file:
        last_download = file.readlines()[-1]
    if float(last_download) <= datetime_last_upload:
        return False
    else:
        return True


def check_file_exists(path: str, filename: str) -> int:
    if os.path.exists(path + filename):
        return os.stat(path + filename).st_size
    return 0


def download_file(file_url: str, file_download: str, timestamp_last_modified: int):
    urllib.request.urlretrieve(file_url, ZIP_FOLDER + file_download, show_progress)
    os.utime(ZIP_FOLDER + file_download, (timestamp_last_modified, timestamp_last_modified))


def check_download(link, file):
    if str(link.get('href')).endswith('.zip') and file in str(link.get('href')):
        file_download: str = link.get('href')
        file_url: str = URL + file_download

        if not file_download.startswith('http'):
            local_file: int = check_file_exists(ZIP_FOLDER, file_download)
            # print(requests.head(file_url).headers)
            file_url_last_upload: list = requests.head(file_url).headers['Last-Modified'].split()

            file_url_last_modified_time: str = str(file_url_last_upload[4]).split(':')
            timestamp_last_modified: int = datetime.datetime(int(file_url_last_upload[3]),
                                                             int(time.strptime(file_url_last_upload[2], '%b').tm_mon),
                                                             int(file_url_last_upload[1]),
                                                             int(file_url_last_modified_time[0]),
                                                             int(file_url_last_modified_time[1]),
                                                             int(file_url_last_modified_time[2])).timestamp()

            # if not check_if_update_base(timestamp_last_modified):
            #     print('Não existem arquivos novos para serem baixados.')
            #     # logging.INFO('Não existem arquivos novos para serem baixados.')
            #     break
            print('Baixando o arquivo: ' + file_download)
            if local_file == 0:
                download_file(file_url, file_download, timestamp_last_modified)
            elif local_file > 0 and local_file != int(requests.head(file_url).headers['Content-Length']):
                download_file(file_url, file_download, timestamp_last_modified)
            else:
                print('O arquivo', file_download, 'esta atualizado.')
        else:
            print('Não foi possível baixar o arquivo: ' + file_download)


def main():
    listDownloadFiles = ['Empresas', 'Estabelecimentos', 'Simples', 'Socios']
    soup: BeautifulSoup = BeautifulSoup(requests.get(URL).text)
    for file in listDownloadFiles:
        for link in soup.find_all('a'):
            check_download(link, file)
    print('Fim!')


if __name__ == '__main__':
    main()
