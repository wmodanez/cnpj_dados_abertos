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
            # Transformações específicas para Simples Nacional
            
            # 1. Tratar valores nulos PRIMEIRO (antes de qualquer conversão)
            null_replacements = {
                'opcao_simples': ['', '0', 'NULL', 'null'],
                'opcao_mei': ['', '0', 'NULL', 'null'],
                'data_opcao_simples': ['0', '00000000', ''],
                'data_exclusao_simples': ['0', '00000000', ''],
                'data_opcao_mei': ['0', '00000000', ''],
                'data_exclusao_mei': ['0', '00000000', '']
            }
            
            for col, null_values in null_replacements.items():
                if col in df.columns:
                    col_dtype = df[col].dtype
                    
                    # Só aplicar se ainda for string
                    if col_dtype == pl.Utf8:
                        df = df.with_columns([
                            pl.when(pl.col(col).is_in(null_values))
                            .then(None)
                            .otherwise(pl.col(col))
                            .alias(col)
                        ])
                    # Para tipos numéricos, converter valores específicos para null
                    elif col_dtype in [pl.Int64, pl.Int32, pl.Int8]:
                        if col.startswith('data_'):
                            # Para datas, 0 significa null
                            df = df.with_columns([
                                pl.when(pl.col(col) == 0)
                                .then(None)
                                .otherwise(pl.col(col))
                                .alias(col)
                            ])
                        else:
                            # Para opções, 0 é um valor válido, não converter para null
                            pass
            
            # 2. Limpar e padronizar CNPJ básico
            if 'cnpj_basico' in df.columns:
                col_dtype = df['cnpj_basico'].dtype
                if col_dtype == pl.Utf8:
                    df = df.with_columns([
                        pl.col('cnpj_basico')
                        .str.replace_all(r'[^\d]', '')  # Remove não-dígitos
                        .str.pad_start(8, '0')          # Garante 8 dígitos, preenchendo com zeros
                        .alias('cnpj_basico')
                    ])
            
            # 3. Converter opções para 0/1 (melhor performance que S/N)
            opcao_columns = [
                'opcao_simples', 'opcao_mei', 'ultimo_evento'
            ]
            
            for col in opcao_columns:
                if col in df.columns:
                    col_dtype = df[col].dtype
                    # Só converter se ainda for string
                    if col_dtype == pl.Utf8:
                        df = df.with_columns([
                            pl.col(col)
                            .map_elements(self._normalize_opcao_to_int, return_dtype=pl.Int8)
                            .alias(col)
                        ])
            
            # 4. Converter datas do Simples Nacional (formato YYYYMMDD)
            date_columns = [
                'data_opcao_simples', 'data_exclusao_simples',
                'data_opcao_mei', 'data_exclusao_mei'
            ]
            
            for col in date_columns:
                if col in df.columns:
                    col_dtype = df[col].dtype
                    
                    # Só aplicar conversão se ainda não for data
                    if col_dtype != pl.Date:
                        # Usar uma abordagem mais direta para converter datas
                        if col_dtype == pl.Utf8:
                            # Para strings, primeiro limpar e validar
                            df = df.with_columns([
                                pl.col(col)
                                .str.replace_all(r'[^\d]', '')  # Remove não-dígitos
                                .map_elements(
                                    self._convert_date_simples_to_date,
                                    return_dtype=pl.Date
                                )
                                .alias(col)
                            ])
                        # Se for numérico (Int64, Int32), converter diretamente
                        elif col_dtype in [pl.Int64, pl.Int32]:
                            df = df.with_columns([
                                pl.col(col)
                                .map_elements(
                                    self._convert_date_simples_to_date,
                                    return_dtype=pl.Date
                                )
                                .alias(col)
                            ])
                        # Para outros tipos, tentar conversão via string
                        else:
                            df = df.with_columns([
                                pl.col(col)
                                .cast(pl.Utf8, strict=False)
                                .map_elements(
                                    self._convert_date_simples_to_date,
                                    return_dtype=pl.Date
                                )
                                .alias(col)
                            ])
            
            # 5. Validar consistência de datas
            df = self._validate_date_consistency(df)
            
            # 6. Adicionar colunas calculadas úteis
            df = self._add_calculated_columns(df)
            
            self.logger.debug(f"Transformações específicas do Simples aplicadas. Linhas: {df.height}")
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
    
    def _convert_date_simples(self, date_str) -> Optional[str]:
        """
        Converte string de data no formato YYYYMMDD para formato ISO.
        
        Args:
            date_str: String da data ou valor numérico
            
        Returns:
            Data convertida em formato ISO (YYYY-MM-DD) ou None
        """
        if date_str is None:
            return None
            
        # Converter para string se necessário
        date_str = str(date_str).strip()
        
        if not date_str or date_str in ['0', '00000000', '', 'None', 'null', 'NULL']:
            return None
        
        try:
            # Garantir que tem 8 dígitos (completar com zeros à esquerda)
            date_str = date_str.zfill(8)
            
            if len(date_str) != 8 or not date_str.isdigit():
                return None
            
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            
            # Validações básicas
            if year < 2006 or year > 2030:  # Simples Nacional criado em 2006
                return None
            if month < 1 or month > 12:
                return None
            if day < 1 or day > 31:
                return None
            
            # Retornar string no formato ISO para o Polars converter
            return f"{year:04d}-{month:02d}-{day:02d}"
            
        except Exception as e:
            # Log do erro seria útil mas pode ser muito verboso
            return None
    
    def _convert_date_simples_to_date(self, date_str):
        """
        Converte string/número de data no formato YYYYMMDD para objeto date do Python.
        
        Args:
            date_str: String da data ou valor numérico
            
        Returns:
            Objeto date do Python ou None
        """
        if date_str is None:
            return None
            
        # Converter para string se necessário
        date_str = str(date_str).strip()
        
        if not date_str or date_str in ['0', '00000000', '', 'None', 'null', 'NULL']:
            return None
        
        try:
            # Garantir que tem 8 dígitos (completar com zeros à esquerda)
            date_str = date_str.zfill(8)
            
            if len(date_str) != 8 or not date_str.isdigit():
                return None
            
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            
            # Validações básicas
            if year < 2006 or year > 2030:  # Simples Nacional criado em 2006
                return None
            if month < 1 or month > 12:
                return None
            if day < 1 or day > 31:
                return None
            
            # Retornar objeto date do Python
            from datetime import date
            return date(year, month, day)
            
        except Exception as e:
            return None
    
    def _validate_date_consistency(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Valida consistência entre datas de opção e exclusão.
        
        Args:
            df: DataFrame a ser validado
            
        Returns:
            DataFrame com datas inconsistentes corrigidas
        """
        # Validar consistência Simples Nacional
        if ('data_opcao_simples' in df.columns and 
            'data_exclusao_simples' in df.columns):
            df = df.with_columns([
                pl.when(
                    (pl.col('data_exclusao_simples').is_not_null()) &
                    (pl.col('data_opcao_simples').is_not_null()) &
                    (pl.col('data_exclusao_simples') <= pl.col('data_opcao_simples'))
                )
                .then(None)  # Remove data de exclusão inválida
                .otherwise(pl.col('data_exclusao_simples'))
                .alias('data_exclusao_simples')
            ])
        
        # Validar consistência MEI
        if ('data_opcao_mei' in df.columns and 
            'data_exclusao_mei' in df.columns):
            df = df.with_columns([
                pl.when(
                    (pl.col('data_exclusao_mei').is_not_null()) &
                    (pl.col('data_opcao_mei').is_not_null()) &
                    (pl.col('data_exclusao_mei') <= pl.col('data_opcao_mei'))
                )
                .then(None)  # Remove data de exclusão inválida
                .otherwise(pl.col('data_exclusao_mei'))
                .alias('data_exclusao_mei')
            ])
        
        return df
    
    def _add_calculated_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Adiciona colunas calculadas úteis.
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame com colunas adicionais
        """
        # Status do Simples Nacional baseado em opções (0/1)
        if 'opcao_simples' in df.columns and 'data_exclusao_simples' in df.columns:
            df = df.with_columns([
                pl.when(
                    (pl.col('opcao_simples') == 1) &  # Optou pelo Simples
                    (pl.col('data_exclusao_simples').is_null())  # Sem data de exclusão
                )
                .then('Ativo no Simples')
                .when(pl.col('opcao_simples') == 1)  # Optou mas foi excluído
                .then('Excluído do Simples') 
                .when(pl.col('opcao_simples') == 0)  # Não optou
                .then('Não optante')
                .otherwise('Status indefinido')
                .alias('status_simples_descricao')
            ])
        
        # Status do MEI baseado em opções (0/1)
        if 'opcao_mei' in df.columns:
            df = df.with_columns([
                pl.when(pl.col('opcao_mei') == 1)
                .then('Optante MEI')
                .when(pl.col('opcao_mei') == 0)
                .then('Não optante MEI')
                .otherwise('MEI indefinido')
                .alias('status_mei_descricao')
            ])
        
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
                'Normalização de opções S/N',
                'Conversão de datas YYYYMMDD',
                'Validação de CNPJ básico',
                'Validação de consistência de datas',
                'Cálculo de situação atual',
                'Tratamento de valores nulos específicos'
            ],
            'output_format': 'Parquet particionado',
            'calculated_columns': [
                'status_simples_descricao',
                'status_mei_descricao'
            ]
        }
        
        return simples_summary 