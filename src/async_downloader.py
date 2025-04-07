import asyncio
import aiohttp
import aiofiles
import os
import logging
from typing import List, Tuple
from urllib.parse import urljoin # Para construir URLs completas

import requests # Para buscar a página de listagem inicial
from bs4 import BeautifulSoup # Para parsear HTML
from dotenv import load_dotenv # Para carregar variáveis de ambiente
from tqdm.asyncio import tqdm # Importar tqdm assíncrono

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração básica de logging (pode ser ajustada conforme necessário)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_zip_urls_from_origin(base_url: str) -> List[str]:
    """Busca URLs de arquivos .zip a partir de uma URL base.

    Assume que a página lista arquivos .zip diretamente ou em subdiretórios (formato Nginx/Apache).
    Pode precisar de adaptação se a estrutura da página for diferente.
    """
    urls = []
    try:
        logger.info(f"Buscando lista de arquivos em: {base_url}")
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Encontra todos os links (tags <a>)
        for link in soup.find_all('a'):
            href = link.get('href')
            if href:
                # Constrói a URL completa
                full_url = urljoin(base_url, href)

                # Verifica se é um arquivo .zip
                if href.lower().endswith('.zip'):
                    if full_url not in urls:
                        logger.debug(f"Encontrado arquivo ZIP: {full_url}")
                        urls.append(full_url)
                # Verifica se é um subdiretório (termina com / e não é o link pai)
                elif href.endswith('/') and href != '../' and not href.startswith('?'):
                    # Recursivamente busca URLs no subdiretório
                    logger.debug(f"Entrando no subdiretório: {full_url}")
                    urls.extend(get_zip_urls_from_origin(full_url))

    except requests.RequestException as e:
        logger.error(f"Erro ao buscar URLs em {base_url}: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao processar {base_url}: {e}")

    logger.info(f"Total de {len(urls)} URLs .zip encontradas.")
    return urls

async def download_file(session: aiohttp.ClientSession, url: str, destination_path: str, semaphore: asyncio.Semaphore, progress_bar: tqdm):
    """
    Downloads a single file asynchronously, updating a progress bar.

    Args:
        session: The aiohttp ClientSession.
        url: The URL of the file to download.
        destination_path: The local path to save the file.
        semaphore: Semaphore to limit concurrent downloads.
        progress_bar: The tqdm progress bar instance for this download.
    """
    async with semaphore:
        # logger.info(f"Iniciando download: {url} para {destination_path}") # Log substituído por tqdm
        try:
            # Lógica para verificar se o arquivo já existe e obter o tamanho para retomar (Range header)
            file_exists = os.path.exists(destination_path)
            resume_header = {}
            initial_size = 0
            file_mode = 'wb'
            if file_exists:
                initial_size = os.path.getsize(destination_path)
                # logger.info(f"Arquivo {destination_path} já existe ({initial_size} bytes). Tentando retomar.") # Log substituído por tqdm
                resume_header = {'Range': f'bytes={initial_size}-'}
                file_mode = 'ab' # Append mode

            async with session.get(url, headers=resume_header, timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)) as response:

                # Se retomando e servidor não suportar (status != 206), baixar do início
                if file_exists and response.status != 206:
                    logger.warning(f"Servidor não suporta retomar {url}. Baixando novamente.")
                    initial_size = 0
                    file_mode = 'wb' # Sobrescrever
                    progress_bar.reset() # Reseta a barra de progresso se baixando novamente
                elif response.status == 206:
                    logger.debug(f"Retomando download de {url} a partir de {initial_size} bytes.")
                elif response.status == 200 and file_exists:
                    logger.warning(f"Arquivo {destination_path} existe mas será sobrescrito (servidor não suportou Range ou erro?).")
                    initial_size = 0
                    file_mode = 'wb'
                    progress_bar.reset()

                response.raise_for_status() # Lança exceção para respostas 4xx/5xx não tratadas acima

                # Obter o tamanho total do CONTEÚDO restante (pode não ser o tamanho total do arquivo se retomando)
                # Ou o tamanho total se Content-Length estiver presente e não estiver retomando
                total_size_header = response.headers.get('Content-Length')
                total_to_download = int(total_size_header) if total_size_header else None

                # Configurar a barra de progresso
                progress_bar.total = total_to_download + initial_size if total_to_download is not None else None # Tamanho total do arquivo final
                progress_bar.update(initial_size) # Atualiza para o tamanho inicial se retomando

                # Remover log inicial redundante se tqdm estiver ativo
                # logger.info(f"Iniciando download: {url} para {destination_path}")

                async with aiofiles.open(destination_path, mode=file_mode) as f:
                    async for chunk in response.content.iter_chunked(8192): # Lê em chunks de 8KB
                        await f.write(chunk)
                        progress_bar.update(len(chunk)) # Atualiza a barra pelo tamanho do chunk

                # Garante que a barra chegue a 100% se o tamanho total era conhecido
                if progress_bar.total and progress_bar.n < progress_bar.total:
                    progress_bar.update(progress_bar.total - progress_bar.n)

                # logger.info(f"Download concluído: {destination_path}") # Log substituído por tqdm
                # TODO: Implementar verificação de integridade (hash) aqui, se necessário
                return destination_path, None # Retorna caminho e nenhum erro

        except aiohttp.ClientError as e:
            logger.error(f"Erro de cliente HTTP ao baixar {url}: {e}")
            progress_bar.set_description(f"Erro HTTP: {url[:50]}...", refresh=True)
            return url, e # Retorna URL e erro
        except asyncio.TimeoutError:
            logger.error(f"Timeout ao baixar {url}")
            progress_bar.set_description(f"Timeout: {url[:50]}...", refresh=True)
            return url, asyncio.TimeoutError("Timeout") # Retorna URL e erro
        except Exception as e:
            logger.error(f"Erro inesperado ao baixar {url}: {e}")
            progress_bar.set_description(f"Erro Inesperado: {url[:50]}...", refresh=True)
            return url, e # Retorna URL e erro
        finally:
            progress_bar.close() # Fecha a barra de progresso individual

async def download_multiple_files(urls: List[str], destination_folder: str, max_concurrent: int = 5):
    """
    Downloads multiple files asynchronously with progress bars.
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
        logger.info(f"Diretório criado: {destination_folder}")

    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = []
    downloaded_files = []
    failed_downloads = []

    progress_bars = {}

    # Usar uma única sessão para todos os downloads
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(urls):
            filename = url.split('/')[-1] # Usando método simples por enquanto
            destination_path = os.path.join(destination_folder, filename)

            # Criar uma barra de progresso tqdm para cada arquivo
            # unit='B', unit_scale=True -> mostra em KB, MB, etc.
            # leave=False -> remove a barra ao concluir (ou True para manter)
            # desc -> descrição curta do arquivo
            pbar = tqdm(total=None, unit='B', unit_scale=True, desc=f"{filename[:30]:<30}", leave=True, position=i)
            progress_bars[url] = pbar

            tasks.append(download_file(session, url, destination_path, semaphore, pbar))

        results = await asyncio.gather(*tasks)
        for result in results:
            file_or_url, error = result
            if error is None:
                downloaded_files.append(file_or_url)
            else:
                # A barra de erro já foi atualizada na função download_file
                failed_downloads.append((file_or_url, error))

    # Limpar barras de progresso que podem não ter sido fechadas por erros inesperados
    for pbar in progress_bars.values():
        if not pbar.disable:
            pbar.close()

    if failed_downloads:
        logger.warning(f"{len(failed_downloads)} downloads falharam.")
        # Os detalhes já foram loggados ou mostrados na barra de progresso

    logger.info(f"Total de downloads concluídos: {len(downloaded_files)}")
    return downloaded_files, failed_downloads

# Exemplo de uso (agora usando .env)
async def main_example():
    # Obter configurações do .env
    base_url = os.getenv('URL_ORIGIN')
    download_folder = os.getenv('PATH_ZIP')

    if not base_url:
        logger.error("Variável de ambiente URL_ORIGIN não definida no arquivo .env")
        return
    if not download_folder:
        logger.error("Variável de ambiente PATH_ZIP não definida no arquivo .env")
        return

    # Buscar as URLs dos arquivos .zip
    zip_urls = get_zip_urls_from_origin(base_url)

    if not zip_urls:
        logger.warning("Nenhuma URL .zip encontrada na origem. Verifique a URL_ORIGIN ou a estrutura da página.")
        return

    # Limitar a quantidade para teste inicial (opcional)
    # zip_urls = zip_urls[:5] # Descomente para testar com poucos arquivos
    logger.info(f"Iniciando download de {len(zip_urls)} arquivos para {download_folder}...")

    # Definir o número máximo de downloads concorrentes
    max_concurrent_downloads = 5 # Ajuste conforme necessário

    downloaded, failed = await download_multiple_files(zip_urls, download_folder, max_concurrent=max_concurrent_downloads)

    print("\n--- Resumo Final ---")
    print(f"Arquivos baixados com sucesso: {len(downloaded)}")

if __name__ == "__main__":
    # Para rodar este exemplo diretamente: python src/async_downloader.py
    # Nota: Em produção, chame download_multiple_files a partir do seu fluxo principal.
    try:
        asyncio.run(main_example())
    except KeyboardInterrupt:
        logger.info("Download interrompido pelo usuário.") 