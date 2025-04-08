def create_parquet_filename(table_name: str, part_number: int) -> str:
    """Cria um nome de arquivo parquet com prefixo da tabela.
    
    Args:
        table_name: Nome da tabela
        part_number: Número da parte do arquivo
        
    Returns:
        str: Nome do arquivo parquet formatado
    """
    return f"{table_name}_{part_number}.parquet"
