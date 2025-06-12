"""
Processador de Simples Nacional refatorado.

Implementação específica para processamento de dados do Simples Nacional,
utilizando a infraestrutura unificada da Fase 2 e integrando
com a entidade Simples da Fase 1.
"""

import logging
import os
import zipfile
import polars as pl
import tempfile
import shutil
from typing import List, Type, Optional, Union

from ...Entity.Simples import Simples
from ...Entity.base import BaseEntity
from ..base.processor import BaseProcessor

logger = logging.getLogger(__name__)


class SimplesProcessor(BaseProcessor):
    """
    Processador específico para dados do Simples Nacional.
    
    Características:
    - Utiliza entidade Simples para validação e transformação
    - Processamento simples sem subsets específicos
    - Integração com sistema de fila unificado
    - Remove toda duplicação de código
    - Transformações específicas para opções S/N e datas
    """
    
    def get_processor_name(self) -> str:
        """Retorna o nome do processador."""
        return "SIMPLES"
    
    def get_entity_class(self) -> Type[BaseEntity]:
        """Retorna a classe de entidade associada."""
        return Simples
    
    def get_valid_options(self) -> List[str]:
        """Retorna opções válidas para este processador."""
        return ['create_private']  # Recebe mas não usa (similar ao socio)
    
    def apply_specific_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica transformações específicas do Simples Nacional.
        
        Args:
            df: DataFrame a ser transformado
            
        Returns:
            DataFrame transformado
        """
        try:
            self.logger.debug(f"Transformações específicas aplicadas. Linhas: {df.height}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao aplicar transformações específicas: {str(e)}")
            return df
    
    def _normalize_opcao_to_int(self, value: str) -> Optional[int]:
        """
        Normaliza valores de opção para 0/1 (melhor performance que S/N).
        
        Args:
            value: Valor a ser normalizado
            
        Returns:
            0 (Não), 1 (Sim) ou None
        """
        if not value or value is None:
            return None
        
        value = str(value).strip().upper()
        
        # Valores que indicam "Sim" -> 1
        if value in ['S', 'SIM', 'YES', '1', 'TRUE', 'VERDADEIRO']:
            return 1
        
        # Valores que indicam "Não" -> 0
        if value in ['N', 'NAO', 'NÃO', 'NO', '0', 'FALSE', 'FALSO']:
            return 0
        
        # Valor não reconhecido -> None
        return None
    
    def _normalize_opcao(self, value: str) -> Optional[str]:
        """
        DEPRECATED: Use _normalize_opcao_to_int para melhor performance.
        Mantido apenas para compatibilidade.
        """
        result = self._normalize_opcao_to_int(value)
        if result == 1:
            return 'S'
        elif result == 0:
            return 'N'
        return None
    
    def _normalize_opcao_sn(self, value) -> Optional[str]:
        """
        Normaliza valores de opção S/N.
        
        Args:
            value: Valor a ser normalizado
            
        Returns:
            'S', 'N' ou None
        """
        if value is None:
            return None
            
        value_str = str(value).strip().upper()
        
        if value_str in ['S', 'SIM', 'YES', '1', 'TRUE']:
            return 'S'
        elif value_str in ['N', 'NAO', 'NÃO', 'NO', '0', 'FALSE']:
            return 'N'
        else:
            return None
    
    def _add_calculated_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Adiciona colunas calculadas simples.
        
        Args:
            df: DataFrame de entrada
            
        Returns:
            DataFrame com colunas calculadas adicionadas
        """
        try:
            # Adicionar status simples baseado apenas na opção
            if 'opcao_simples' in df.columns:
                df = df.with_columns([
                    pl.when(pl.col('opcao_simples') == 'S')
                    .then(pl.lit('ATIVO'))
                    .when(pl.col('opcao_simples') == 'N')
                    .then(pl.lit('INATIVO'))
                    .otherwise(pl.lit('INDEFINIDO'))
                    .alias('status_simples')
                ])
            
            # Adicionar status MEI baseado apenas na opção
            if 'opcao_mei' in df.columns:
                df = df.with_columns([
                    pl.when(pl.col('opcao_mei') == 'S')
                    .then(pl.lit('ATIVO'))
                    .when(pl.col('opcao_mei') == 'N')
                    .then(pl.lit('INATIVO'))
                    .otherwise(pl.lit('INDEFINIDO'))
                    .alias('status_mei')
                ])
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao adicionar colunas calculadas: {str(e)}")
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
        Implementação específica do processamento de um ZIP do Simples Nacional.
        
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
                        
                        if df is not None and isinstance(df, pl.DataFrame) and not df.is_empty():
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
                chunk_filename = f"{zip_prefix}_simples_chunk_{i+1:03d}.parquet"
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
        Retorna resumo do processamento específico para Simples Nacional.
        
        Returns:
            Dict com informações de resumo
        """
        base_summary = self.get_status()
        
        # Adicionar informações específicas do Simples Nacional
        simples_summary = {
            **base_summary,
            'entity_type': 'Simples',
            'supports_chunking': True,
            'default_chunk_size': 500_000,
            'specific_transformations': [
                'Conversão de datas YYYYMMDD',
                'Validação de consistência de datas',
                'Cálculo de situação atual',
                'Tratamento de valores nulos específicos'
            ],
            'output_format': 'Parquet particionado',
            'calculated_columns': [
                'status_simples',
                'status_mei'
            ]
        }
        
        return simples_summary 