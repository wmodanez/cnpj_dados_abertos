def create_parquet_filename(table_name: str, part_number: int) -> str:
    """Cria um nome de arquivo parquet com prefixo da tabela.
    
    Args:
        table_name: Nome da tabela
        part_number: Número da parte do arquivo
        
    Returns:
        str: Nome do arquivo parquet formatado
    """
    # Formatar com 3 dígitos para manter ordenação dos arquivos consistente
    # e garantir que não ocorram conflitos para arquivos grandes
    return f"{table_name}_{part_number:03d}.parquet"
