"""
Funções relacionadas à criação e verificação de pastas.
"""
import logging
import os


def check_basic_folders(folders: list) -> None:
    """Verifica e cria as pastas básicas necessárias para o funcionamento do sistema."""
    logger = logging.getLogger(__name__)

    for folder in folders:
        if not os.path.exists(folder):
            try:
                os.makedirs(folder)
                logger.info(f'Pasta {folder} criada com sucesso')
            except Exception as e:
                logger.error(f'Erro ao criar pasta {folder}: {str(e)}')
                raise


def get_output_path(base_path: str, table_name: str) -> str:
    """
    Retorna o caminho correto para salvar arquivos, mantendo a estrutura de pastas.
    
    Esta função garante que a estrutura de pastas siga o padrão:
    PATH_PARQUET/[nome-da-pasta-remota]/[tipo-de-dado]
    
    Args:
        base_path: Caminho base (já deve conter a pasta remota)
        table_name: Nome da tabela/tipo de dado (empresas, socios, etc.)
        
    Returns:
        str: Caminho completo para salvar os arquivos
    """
    logger = logging.getLogger(__name__)
    logger.info(f"get_output_path recebeu: base_path={base_path}, table_name={table_name}")
    
    output_dir = os.path.join(base_path, table_name)
    logger.info(f"get_output_path criando diretório: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def ensure_correct_folder_structure(path_parquet: str, remote_folder: str, table_name: str) -> str:
    """
    Garante que o caminho de saída siga a estrutura PATH_PARQUET/REMOTE_FOLDER/TABLE_NAME.
    
    Esta função resolve o problema de estrutura de pastas, certificando-se de que os arquivos
    Parquet são salvos na estrutura correta.
    
    Args:
        path_parquet: Caminho base do diretório parquet
        remote_folder: Nome da pasta remota (AAAA-MM)
        table_name: Nome da tabela/tipo de dado (empresas, socios, etc.)
        
    Returns:
        str: Caminho completo para a pasta de saída
    """
    logger = logging.getLogger(__name__)
    
    # Verificar se path_parquet já contém remote_folder
    parts = path_parquet.rstrip('/\\').split(os.path.sep)
    
    # Se remote_folder já está no caminho, não o adicione novamente
    if remote_folder in parts:
        output_dir = os.path.join(path_parquet, table_name)
        logger.info(f"ensure_correct_folder_structure: remote_folder '{remote_folder}' já está no caminho. Output: {output_dir}")
    else:
        output_dir = os.path.join(path_parquet, remote_folder, table_name)
        logger.info(f"ensure_correct_folder_structure: adicionando remote_folder '{remote_folder}' ao caminho. Output: {output_dir}")
    
    os.makedirs(output_dir, exist_ok=True)
    return output_dir
