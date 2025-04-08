"""
Funções relacionadas à criação e verificação de pastas.
"""
import logging
import os


def check_basic_folders(folder: str) -> None:
    """Verifica e cria as pastas básicas necessárias para o funcionamento do sistema."""
    logger = logging.getLogger(__name__)

    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
            logger.info(f'Pasta {folder} criada com sucesso')
        except Exception as e:
            logger.error(f'Erro ao criar pasta {folder}: {str(e)}')
            raise
