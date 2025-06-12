"""
Processador de sócios refatorado.

Implementação específica para processamento de dados de sócios,
utilizando a infraestrutura unificada da Fase 2 e integrando
com a entidade Socio da Fase 1.
"""

import logging
import os
import zipfile
import polars as pl
import tempfile
from typing import List, Type

from ...Entity.Socio import Socio
from ...Entity.base import BaseEntity
from ..base.processor import BaseProcessor

logger = logging.getLogger(__name__)


class SocioProcessor(BaseProcessor):
    """
    Processador específico para dados de sócios.
    
    Características:
    - Utiliza entidade Socio para validação e transformação
    - Processamento simples sem subsets específicos
    - Integração com sistema de fila unificado
    - Remove toda duplicação de código
    """
    
    def get_processor_name(self) -> str:
        """Retorna o nome do processador."""
        return "SOCIO"
    
    def get_entity_class(self) -> Type[BaseEntity]:
        """Retorna a classe de entidade associada."""
        return Socio
    
    def get_valid_options(self) -> List[str]:
        """Retorna opções válidas para este processador."""
        return ['create_private']  # Recebe mas não usa
    
    def apply_specific_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica transformações específicas de sócios.
        
        Args:
            df: DataFrame a ser transformado
            
        Returns:
            DataFrame transformado
        """
        try:
            # Transformações específicas para sócios
            
            # 1. Normalizar nomes
            name_columns = ['nome_socio', 'nome_representante']
            for col in name_columns:
                if col in df.columns:
                    df = df.with_columns([
                        pl.col(col)
                        .str.strip_chars()
                        .str.to_uppercase()
                        .alias(col)
                    ])
            
            # 2. Tratar valores nulos específicos
            null_replacements = {
                'representante_legal': ['0', '00000000000'],
                'qualificacao_socio': [0, 99],
                'qualificacao_representante_legal': [0, 99]
            }
            
            for col, null_values in null_replacements.items():
                if col in df.columns:
                    df = df.with_columns([
                        pl.when(pl.col(col).is_in(null_values))
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    ])

            
            if 'representante_legal' in df.columns:
                df = df.with_columns([
                    pl.when(pl.col('representante_legal').str.len_chars() != 11)
                    .then(None)
                    .otherwise(pl.col('representante_legal'))
                    .alias('representante_legal')
                ])
            
            self.logger.debug(f"Transformações específicas aplicadas. Linhas: {df.height}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao aplicar transformações específicas: {str(e)}")
            return df
    
    def process_single_zip_impl(
        self, 
        zip_file: str, 
        path_zip: str, 
        path_unzip: str, 
        path_parquet: str, 
        **kwargs
    ) -> bool:
        """
        Implementação específica do processamento de um ZIP de sócios.
        
        Args:
            zip_file: Nome do arquivo ZIP
            path_zip: Diretório com arquivos ZIP
            path_unzip: Diretório para extração
            path_parquet: Diretório de saída
            **kwargs: Opções adicionais
            
        Returns:
            bool: True se sucesso, False caso contrário
        """
        zip_path = os.path.join(path_zip, zip_file)
        zip_prefix = os.path.splitext(zip_file)[0]
        
        try:
            self.logger.info(f"Iniciando processamento de {zip_file}")
            
            # Verificar se arquivo existe
            if not os.path.exists(zip_path):
                self.logger.error(f"Arquivo ZIP não encontrado: {zip_path}")
                return False
            
            # Criar diretório temporário para extração
            with tempfile.TemporaryDirectory() as temp_extract_dir:
                
                # 1. Extrair arquivo ZIP
                if not self._extract_zip(zip_path, temp_extract_dir):
                    self.logger.error(f"Falha ao extrair {zip_file}")
                    return False
                
                # 2. Processar arquivos extraídos
                all_dataframes = []
                
                for file_name in os.listdir(temp_extract_dir):
                    file_path = os.path.join(temp_extract_dir, file_name)
                    
                    if os.path.isfile(file_path):
                        self.logger.debug(f"Processando arquivo: {file_name}")
                        
                        # Processar arquivo de dados
                        df = self.process_data_file(file_path)
                        
                        if df is not None and not df.is_empty():
                            # Aplicar transformações da entidade + específicas
                            df = self.apply_entity_transformations(df)
                            all_dataframes.append(df)
                            self.logger.debug(f"Arquivo {file_name} processado: {df.height} linhas")
                        else:
                            self.logger.warning(f"Arquivo {file_name} não pôde ser processado")
                
                # 3. Combinar todos os DataFrames
                if not all_dataframes:
                    self.logger.warning(f"Nenhum dado válido encontrado em {zip_file}")
                    return False
                
                # Concatenar DataFrames
                combined_df = pl.concat(all_dataframes, how="vertical_relaxed")
                self.logger.info(f"Dados combinados: {combined_df.height} linhas")
                
                # 4. Salvar como Parquet
                success = self.create_parquet_output(
                    combined_df, 
                    path_parquet, 
                    zip_prefix,
                    partition_size=500_000
                )
                
                if success:
                    self.logger.info(f"✓ Processamento de {zip_file} concluído com sucesso")
                    return True
                else:
                    self.logger.error(f"✗ Falha ao salvar dados de {zip_file}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Erro no processamento de {zip_file}: {str(e)}")
            return False
    
    def _extract_zip(self, zip_path: str, extract_dir: str) -> bool:
        """
        Extrai arquivo ZIP.
        
        Args:
            zip_path: Caminho do arquivo ZIP
            extract_dir: Diretório de extração
            
        Returns:
            bool: True se sucesso, False caso contrário
        """
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            self.logger.debug(f"ZIP extraído com sucesso: {os.path.basename(zip_path)}")
            return True
            
        except zipfile.BadZipFile:
            self.logger.error(f"Arquivo ZIP corrompido: {os.path.basename(zip_path)}")
            return False
        except Exception as e:
            self.logger.error(f"Erro ao extrair ZIP {os.path.basename(zip_path)}: {str(e)}")
            return False
    
    def process_in_chunks(
        self, 
        data_file_path: str, 
        output_dir: str, 
        zip_prefix: str, 
        chunk_size: int = 500_000
    ) -> int:
        """
        Processa arquivo grande em chunks.
        
        Args:
            data_file_path: Caminho do arquivo de dados
            output_dir: Diretório de saída
            zip_prefix: Prefixo para nomes dos arquivos
            chunk_size: Tamanho do chunk
            
        Returns:
            int: Número de chunks processados
        """
        try:
            self.logger.info(f"Processando arquivo em chunks: {os.path.basename(data_file_path)}")
            
            chunk_counter = 0
            
            # Usar scan_csv para processamento lazy
            lazy_df = pl.scan_csv(
                data_file_path,
                separator=';',
                has_header=False,
                encoding='utf8-lossy',
                ignore_errors=True,
                truncate_ragged_lines=True
            )
            
            # Processar em batches
            total_rows = lazy_df.select(pl.count()).collect().item()
            num_chunks = (total_rows + chunk_size - 1) // chunk_size
            
            self.logger.info(f"Total de linhas: {total_rows}, Chunks: {num_chunks}")
            
            for i in range(num_chunks):
                start_row = i * chunk_size
                
                # Coletar chunk
                chunk_df = lazy_df.slice(start_row, chunk_size).collect()
                
                if chunk_df.is_empty():
                    continue
                
                # Aplicar transformações
                chunk_df = self.apply_entity_transformations(chunk_df)
                
                # Salvar chunk
                chunk_filename = f"{zip_prefix}_socio_chunk_{i+1:03d}.parquet"
                chunk_path = os.path.join(output_dir, chunk_filename)
                
                chunk_df.write_parquet(chunk_path, compression='snappy')
                chunk_counter += 1
                
                self.logger.debug(f"Chunk {i+1}/{num_chunks} salvo: {chunk_df.height} linhas")
            
            self.logger.info(f"Processamento em chunks concluído: {chunk_counter} chunks")
            return chunk_counter
            
        except Exception as e:
            self.logger.error(f"Erro no processamento em chunks: {str(e)}")
            return 0
    
    def get_processing_summary(self) -> dict:
        """
        Retorna resumo do processamento específico para sócios.
        
        Returns:
            Dict com informações de resumo
        """
        base_summary = self.get_status()
        
        # Adicionar informações específicas de sócios
        socio_summary = {
            **base_summary,
            'entity_type': 'Socio',
            'supports_chunking': True,
            'default_chunk_size': 500_000,
            'specific_transformations': [
                'Normalização de nomes',
                'Validação de comprimento de documentos',
                'Tratamento de valores nulos específicos'
            ],
            'output_format': 'Parquet particionado'
        }
        
        return socio_summary 