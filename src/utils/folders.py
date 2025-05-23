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
    
    # Normalizar o caminho para trabalhar com uma forma padrão
    path_normalized = os.path.normpath(path_parquet)
    parts = path_normalized.split(os.path.sep)
    
    # Verificar se temos um padrão de duplicação como "parquet/2025-03/2025-03"
    duplicate_pattern = f"{remote_folder}{os.path.sep}{remote_folder}"
    
    if duplicate_pattern in path_normalized:
        # Remover a duplicação - separar o caminho até a primeira ocorrência de remote_folder
        base_path = path_normalized.split(remote_folder)[0] + remote_folder
        output_dir = os.path.join(base_path, table_name)
        logger.info(f"ensure_correct_folder_structure: removida duplicação de '{remote_folder}' no caminho. Output: {output_dir}")
    # Caso onde remote_folder está no final do caminho
    elif parts and parts[-1] == remote_folder:
        output_dir = os.path.join(path_parquet, table_name)
        logger.info(f"ensure_correct_folder_structure: remote_folder '{remote_folder}' já é o último componente do caminho. Output: {output_dir}")
    # Caso onde remote_folder está presente em alguma parte do caminho, mas não duplicado
    elif remote_folder in parts:
        # Verificar se remote_folder é seguido pelo table_name (para não adicionar table_name duas vezes)
        rf_index = parts.index(remote_folder)
        if rf_index + 1 < len(parts) and parts[rf_index + 1] == table_name:
            # Remote folder já é seguido pelo table_name, não precisa adicionar
            output_dir = path_parquet
            logger.info(f"ensure_correct_folder_structure: remote_folder '{remote_folder}' seguido de table_name já existe no caminho. Output: {output_dir}")
        else:
            # Remote folder existe mas não é seguido pelo table_name
            output_dir = os.path.join(path_parquet, table_name)
            logger.info(f"ensure_correct_folder_structure: remote_folder '{remote_folder}' já existe no caminho. Output: {output_dir}")
    else:
        # Remote folder não está no caminho, adicione-o
        output_dir = os.path.join(path_parquet, remote_folder, table_name)
        logger.info(f"ensure_correct_folder_structure: adicionando remote_folder '{remote_folder}' ao caminho. Output: {output_dir}")
    
    os.makedirs(output_dir, exist_ok=True)
    return output_dir
