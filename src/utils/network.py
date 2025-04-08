"""
Utilitários para operações de rede.
"""
import logging
import socket
import time
from typing import Tuple

import requests

logger = logging.getLogger(__name__)


def check_internet_connection(test_url: str = "https://www.gov.br", timeout: int = 5, max_retries: int = 3) -> Tuple[
    bool, str]:
    """
    Verifica se há conexão com a internet tentando acessar uma URL específica.
    
    Args:
        test_url: URL para testar a conexão (padrão: site do governo brasileiro)
        timeout: Tempo limite em segundos para a requisição
        max_retries: Número máximo de tentativas antes de desistir
        
    Returns:
        Tupla (bool, str): Status da conexão e mensagem descritiva
    """
    retry_count = 0

    while retry_count < max_retries:
        try:
            # Primeiro verifica se a resolução DNS está funcionando
            try:
                socket.gethostbyname("www.google.com")
            except socket.gaierror:
                logger.warning(f"Falha na resolução DNS. Tentativa {retry_count + 1}/{max_retries}")
                retry_count += 1
                time.sleep(2)  # Espera 2 segundos antes de tentar novamente
                continue

            # Tenta fazer uma requisição HTTP à URL de teste
            response = requests.get(test_url, timeout=timeout)

            if response.status_code >= 200 and response.status_code < 300:
                logger.info(f"Conexão com a internet verificada com sucesso (status: {response.status_code})")
                return True, "Conexão com a internet está funcionando normalmente"
            else:
                logger.warning(f"Conexão com a internet instável. Status HTTP: {response.status_code}")
                retry_count += 1
                time.sleep(2)

        except requests.ConnectionError:
            logger.warning(f"Erro de conexão ao tentar acessar {test_url}. Tentativa {retry_count + 1}/{max_retries}")
            retry_count += 1
            time.sleep(2)

        except requests.Timeout:
            logger.warning(f"Timeout ao tentar acessar {test_url}. Tentativa {retry_count + 1}/{max_retries}")
            retry_count += 1
            time.sleep(2)

        except Exception as e:
            logger.error(f"Erro inesperado ao verificar conexão: {str(e)}. Tentativa {retry_count + 1}/{max_retries}")
            retry_count += 1
            time.sleep(2)

    logger.error(f"Falha ao verificar conexão com a internet após {max_retries} tentativas")
    return False, "Não foi possível estabelecer conexão com a internet"
