"""
Processador de empresas refatorado.

Implementação específica para processamento de dados de empresas,
utilizando a infraestrutura unificada da Fase 2 e integrando
com a entidade Empresa da Fase 1.

Funcionalidades específicas:
- Extração de CPF da razão social
- Criação de subset de empresas privadas (create_private)
"""

import logging
import os
import zipfile
import polars as pl
import tempfile
import shutil
from typing import List, Type

from ...Entity.Empresa import Empresa
from ...Entity.base import BaseEntity
from ..base.processor import BaseProcessor

logger = logging.getLogger(__name__)


class EmpresaProcessor(BaseProcessor):
    """
    Processador específico para dados de empresas.
    
    Características:
    - Utiliza entidade Empresa para validação e transformação
    - Extração automática de CPF da razão social
    - Funcionalidade create_private para subset de empresas privadas
    - Integração com sistema de fila unificado
    - Remove toda duplicação de código
    """
    
    def get_processor_name(self) -> str:
        """Retorna o nome do processador."""
        return "EMPRESA"
    
    def get_entity_class(self) -> Type[BaseEntity]:
        """Retorna a classe de entidade associada."""
        return Empresa
    
    def get_valid_options(self) -> List[str]:
        """Retorna opções válidas para este processador."""
        return ['create_private']  # Funcionalidade específica de empresas
    
    def apply_specific_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica transformações específicas de empresas.
        
        Args:
            df: DataFrame a ser transformado
            
        Returns:
            DataFrame transformado
        """
        try:
            # 1. Extrair CPF da razão social (se ainda não foi processado)
            if 'razao_social' in df.columns and 'cpf_extraido' not in df.columns:
                df = df.with_columns([
                    pl.col('razao_social')
                    .str.extract(r'(\d{11})', group_index=1)  # Extrai CPF
                    .alias('cpf_extraido')
                ])
                
                # Limpar razão social removendo CPF
                df = df.with_columns([
                    pl.col('razao_social')
                    .str.replace_all(r'\d{11}', '')  # Remove CPF
                    .str.strip_chars()
                    .str.to_uppercase()
                    .alias('razao_social')
                ])
            
            # 2. Normalizar strings (apenas se ainda são strings)
            string_columns = ['razao_social', 'ente_federativo_responsavel']
            for col in string_columns:
                if col in df.columns:
                    col_dtype = df[col].dtype
                    if col_dtype == pl.Utf8:  # Só aplicar se ainda for string
                        df = df.with_columns([
                            pl.col(col)
                            .str.strip_chars()
                            .str.to_uppercase()
                            .alias(col)
                        ])
            
            # 3. Validar CPF extraído
            if 'cpf_extraido' in df.columns:
                invalid_cpfs = [
                    "00000000000", "11111111111", "22222222222", "33333333333",
                    "44444444444", "55555555555", "66666666666", "77777777777",
                    "88888888888", "99999999999"
                ]
                
                df = df.with_columns([
                    pl.when(
                        pl.col('cpf_extraido').is_null() |
                        (pl.col('cpf_extraido').str.len_chars() != 11) |
                        (pl.col('cpf_extraido').is_in(invalid_cpfs))
                    )
                    .then(None)
                    .otherwise(pl.col('cpf_extraido'))
                    .alias('cpf_extraido')
                ])
            
            # 4. Validar porte da empresa (apenas validação, sem conversão para texto)
            if 'porte_empresa' in df.columns:
                df = df.with_columns([
                    pl.when(
                        (pl.col('porte_empresa') < 1) |
                        (pl.col('porte_empresa') > 5)
                    )
                    .then(None)
                    .otherwise(pl.col('porte_empresa'))
                    .alias('porte_empresa')
                ])
                        
            self.logger.debug(f"Transformações específicas de empresas aplicadas. Linhas: {df.height}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao aplicar transformações específicas: {str(e)}")
            return df
    
    def create_private_subset(self, df: pl.DataFrame, output_path: str, zip_prefix: str) -> bool:
        """
        Cria subset de empresas privadas baseado na natureza jurídica.
        
        Args:
            df: DataFrame principal
            output_path: Caminho de saída
            zip_prefix: Prefixo do arquivo ZIP
            
        Returns:
            bool: True se criou com sucesso
        """
        try:
            # Códigos de natureza jurídica para empresas privadas (principais)
            private_codes = [
                206,  # Sociedade Empresária Limitada
                213,  # Empresário (Individual)
                230,  # Sociedade Anônima Fechada
                231,  # Sociedade Anônima Aberta
                224,  # Sociedade Simples Limitada
                201,  # Empresa Individual de Responsabilidade Limitada (EIRELI)
                214,  # Produtor Rural (Pessoa Física)
                267,  # Sociedade em Nome Coletivo
                228,  # Cooperativa
                # Adicionar outros códigos conforme necessário
            ]
            
            # Filtrar apenas empresas privadas
            df_private = df.filter(pl.col("natureza_juridica").is_in(private_codes))
            
            if df_private.height == 0:
                self.logger.warning(f"Nenhuma empresa privada encontrada para {zip_prefix}")
                return True
            
            # Criar subdiretório para empresas privadas
            private_output_path = os.path.join(output_path, "empresa_privada")
            os.makedirs(private_output_path, exist_ok=True)
            
            self.logger.info(f"Criando subset de empresas privadas: {df_private.height} empresas")
            
            # Salvar subset
            success = self.create_parquet_output(
                df_private, 
                private_output_path, 
                f"{zip_prefix}_privada",
                partition_size=500_000
            )
            
            if success:
                self.logger.info(f"Subset de empresas privadas salvo com sucesso")
            else:
                self.logger.error(f"Falha ao salvar subset de empresas privadas")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Erro ao criar subset de empresas privadas: {e}")
            return False
    
    def process_single_zip_impl(
        self, 
        zip_file: str, 
        path_zip: str, 
        path_unzip: str, 
        path_parquet: str, 
        **kwargs
    ) -> bool:
        """
        Implementação específica do processamento de um ZIP de empresas.
        
        Args:
            zip_file: Nome do arquivo ZIP
            path_zip: Diretório com arquivos ZIP
            path_unzip: Diretório para extração
            path_parquet: Diretório de saída
            **kwargs: Opções adicionais (incluindo create_private)
            
        Returns:
            bool: True se sucesso, False caso contrário
        """
        zip_path = os.path.join(path_zip, zip_file)
        zip_prefix = os.path.splitext(zip_file)[0]
        create_private = kwargs.get('create_private', False)
        
        try:
            self.logger.info(f"Iniciando processamento de {zip_file} (create_private={create_private})")
            
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
                
                # 4. Salvar como Parquet principal
                success = self.create_parquet_output(
                    combined_df, 
                    path_parquet, 
                    zip_prefix,
                    partition_size=500_000
                )
                
                if not success:
                    self.logger.error(f"✗ Falha ao salvar dados principais de {zip_file}")
                    return False
                
                # 5. Criar subset de empresas privadas se solicitado
                if create_private:
                    self.logger.info(f"Criando subset de empresas privadas para {zip_file}")
                    private_success = self.create_private_subset(combined_df, path_parquet, zip_prefix)
                    
                    if not private_success:
                        self.logger.warning(f"Falha ao criar subset privado para {zip_file}")
                        # Não falha o processamento principal por causa disso
                
                self.logger.info(f"✓ Processamento de {zip_file} concluído com sucesso")
                return True
                    
        except Exception as e:
            self.logger.error(f"Erro no processamento de {zip_file}: {str(e)}")
            return False
    
    def _extract_zip(self, zip_path: str, extract_dir: str) -> bool:
        """
        Extrai arquivo ZIP usando método paralelo quando possível.
        
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
        chunk_size: int = 500_000,
        create_private: bool = False
    ) -> int:
        """
        Processa arquivo grande em chunks.
        
        Args:
            data_file_path: Caminho do arquivo de dados
            output_dir: Diretório de saída
            zip_prefix: Prefixo para nomes dos arquivos
            chunk_size: Tamanho do chunk
            create_private: Se deve criar subset privado
            
        Returns:
            int: Número de chunks processados
        """
        try:
            self.logger.info(f"Processando arquivo em chunks: {os.path.basename(data_file_path)}")
            
            chunk_counter = 0
            all_private_dfs = []  # Para acumular dados privados
            
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
                
                # Salvar chunk principal
                chunk_filename = f"{zip_prefix}_empresa_chunk_{i+1:03d}.parquet"
                chunk_path = os.path.join(output_dir, chunk_filename)
                
                chunk_df.write_parquet(chunk_path, compression='snappy')
                chunk_counter += 1
                
                # Se deve criar subset privado, filtrar e acumular
                if create_private and 'natureza_juridica' in chunk_df.columns:
                    private_codes = [206, 213, 230, 231, 224, 201, 214, 267, 228]
                    private_chunk = chunk_df.filter(pl.col("natureza_juridica").is_in(private_codes))
                    
                    if private_chunk.height > 0:
                        all_private_dfs.append(private_chunk)
                
                self.logger.debug(f"Chunk {i+1}/{num_chunks} salvo: {chunk_df.height} linhas")
            
            # Salvar subset privado acumulado se solicitado
            if create_private and all_private_dfs:
                combined_private = pl.concat(all_private_dfs, how="vertical_relaxed")
                self.create_private_subset(combined_private, output_dir, zip_prefix)
            
            self.logger.info(f"Processamento em chunks concluído: {chunk_counter} chunks")
            return chunk_counter
            
        except Exception as e:
            self.logger.error(f"Erro no processamento em chunks: {str(e)}")
            return 0
    
    def get_processing_summary(self) -> dict:
        """
        Retorna resumo do processamento específico para empresas.
        
        Returns:
            Dict com informações de resumo
        """
        base_summary = self.get_status()
        
        # Adicionar informações específicas de empresas
        empresa_summary = {
            **base_summary,
            'entity_type': 'Empresa',
            'supports_chunking': True,
            'default_chunk_size': 500_000,
            'specific_transformations': [
                'Extração de CPF da razão social',
                'Limpeza da razão social',
                'Normalização de strings',
                'Validação de porte da empresa',
                'Validação de CPF extraído'
            ],
            'output_format': 'Parquet particionado',
            'calculated_columns': [
                'cpf_extraido',
                'is_empresa_privada'
            ],
            'special_features': [
                'Subset de empresas privadas (create_private)',
                'Extração automática de CPF',
            ]
        }
        
        return empresa_summary 