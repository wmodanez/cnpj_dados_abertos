"""
Classe base abstrata para processadores unificados.

Este módulo define a interface base que todos os processadores devem implementar,
integrando com as entidades da Fase 1 e utilizando o sistema de fila unificado.
"""

import logging
import os
import polars as pl
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Type, List
from pathlib import Path
import tempfile
import zipfile

# Imports das entidades (Fase 1)
from ...Entity.base import BaseEntity
from ...Entity import Empresa, Estabelecimento, Socio, Simples

# Import condicional da config
try:
    from ...config import config
except ImportError:
    # Criar config básica se não existir
    class Config:
        class file:
            separator = ';'
            encoding = 'latin1'
    config = Config()

# Imports da infraestrutura unificada (Fase 2)
from .queue_manager import ProcessingQueueManager
from .resource_monitor import ResourceMonitor
from ...utils import delete_zip_after_extraction


class BaseProcessor(ABC):
    """
    Classe base abstrata para todos os processadores.
    
    Integra com:
    - Entidades da Fase 1 (validação e transformação)  
    - Sistema de fila unificado da Fase 2
    - Monitor de recursos centralizado
    
    Substitui a lógica duplicada presente nos 4 processadores originais.
    """
    
    def __init__(self, path_zip: str, path_unzip: str, path_parquet: str, **kwargs):
        """
        Inicializa o processador base.
        
        Args:
            path_zip: Diretório com arquivos ZIP
            path_unzip: Diretório para extração
            path_parquet: Diretório de saída
            **kwargs: Opções específicas do processador
        """
        self.path_zip = path_zip
        self.path_unzip = path_unzip
        self.path_parquet = path_parquet
        self.options = kwargs
        
        # Opção para deletar ZIPs após extração
        self.delete_zips_after_extract = kwargs.get('delete_zips_after_extract', False)
        
        # Configurar logging
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Validar opções
        self._validate_options()
        
        # Inicializar componentes unificados
        self.resource_monitor = ResourceMonitor()
        self.queue_manager = ProcessingQueueManager(
            processor_name=self.get_processor_name(),
            max_workers=kwargs.get('max_workers')
        )
        
        # Log da inicialização
        self.logger.info(f"Processador {self.get_processor_name()} inicializado")
        self.logger.info(f"Caminhos: ZIP={path_zip}, UNZIP={path_unzip}, PARQUET={path_parquet}")
        self.logger.info(f"Deleção de ZIPs após extração: {'ATIVADA' if self.delete_zips_after_extract else 'DESATIVADA'}")
        self.logger.info(f"Opções: {self.options}")
    
    # Métodos abstratos que cada processador deve implementar
    
    @abstractmethod
    def get_processor_name(self) -> str:
        """Retorna o nome do processador (ex: 'EMPRESA', 'SOCIO')."""
        pass
    
    @abstractmethod
    def get_entity_class(self) -> Type[BaseEntity]:
        """Retorna a classe de entidade associada ao processador."""
        pass
    
    @abstractmethod
    def get_valid_options(self) -> List[str]:
        """Retorna lista de opções válidas para este processador."""
        pass
    
    @abstractmethod
    def apply_specific_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica transformações específicas do processador."""
        pass
    
    @abstractmethod
    def process_single_zip_impl(
        self, 
        zip_file: str, 
        path_zip: str, 
        path_unzip: str, 
        path_parquet: str, 
        **kwargs
    ) -> bool:
        """Implementação específica do processamento de um ZIP."""
        pass
    
    # Métodos concretos compartilhados
    
    def _validate_options(self) -> None:
        """Valida opções fornecidas contra opções válidas."""
        valid_options = self.get_valid_options()
        
        # Adicionar opções globais válidas
        global_valid_options = ['max_workers', 'delete_zips_after_extract']
        
        for option in self.options:
            if option not in valid_options and option not in global_valid_options:
                self.logger.warning(f"Opção '{option}' não é válida para {self.get_processor_name()}")
    
    def get_column_mapping(self, df: pl.DataFrame) -> Dict[str, str]:
        """
        Mapeia colunas do DataFrame para nomes da entidade.
        
        Args:
            df: DataFrame com colunas nomeadas como column_1, column_2, etc.
            
        Returns:
            Dict mapeando colunas antigas para nomes das entidades
        """
        entity_class = self.get_entity_class()
        entity_columns = entity_class.get_column_names()
        df_columns = df.columns
        
        mapping = {}
        for i, entity_col in enumerate(entity_columns):
            df_col = f"column_{i+1}"
            if df_col in df_columns:
                mapping[df_col] = entity_col
        
        return mapping
    
    def apply_entity_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica transformações usando a entidade associada.
        
        Args:
            df: DataFrame a ser transformado
            
        Returns:
            DataFrame transformado
        """
        try:
            entity_class = self.get_entity_class()
            
            # 1. Renomear colunas para corresponder à entidade
            column_mapping = self.get_column_mapping(df)
            if column_mapping:
                rename_exprs = []
                for old_col, new_col in column_mapping.items():
                    if old_col in df.columns:
                        rename_exprs.append(pl.col(old_col).alias(new_col))
                
                if rename_exprs:
                    df = df.select(rename_exprs)
            
            # 2. Converter tipos conforme definido na entidade
            df = self._convert_entity_types(df, entity_class)
            
            # 3. Aplicar transformações específicas da entidade
            transformations = entity_class.get_transformations()
            for transformation in transformations:
                df = self._apply_entity_transformation(df, transformation, entity_class)
            
            # 4. Aplicar transformações específicas do processador
            df = self.apply_specific_transformations(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao aplicar transformações da entidade: {str(e)}")
            return df
    
    def _convert_entity_types(self, df: pl.DataFrame, entity_class: Type[BaseEntity]) -> pl.DataFrame:
        """Converte tipos conforme definido na entidade."""
        try:
            type_mapping = entity_class.get_column_types()
            
            conversions = []
            for col_name, col_type in type_mapping.items():
                if col_name in df.columns:
                    conversions.append(
                        pl.col(col_name).cast(col_type, strict=False).alias(col_name)
                    )
            
            if conversions:
                df = df.with_columns(conversions)
            
            return df
            
        except Exception as e:
            self.logger.warning(f"Erro ao converter tipos: {str(e)}")
            return df
    
    def _apply_entity_transformation(
        self, 
        df: pl.DataFrame, 
        transformation: str, 
        entity_class: Type[BaseEntity]
    ) -> pl.DataFrame:
        """Aplica uma transformação específica baseada na entidade."""
        try:
            # Aqui você pode implementar transformações específicas
            # Por enquanto, retorna o DataFrame sem modificação
            # TODO: Implementar transformações baseadas no nome
            
            if transformation == 'convert_dates':
                # Exemplo de transformação de datas
                date_columns = ['data_entrada_sociedade', 'data_situacao_cadastral', 'data_inicio_atividade']
                for col in date_columns:
                    if col in df.columns:
                        df = df.with_columns([
                            pl.col(col).str.strptime(pl.Date, "%Y%m%d", strict=False).alias(col)
                        ])
            
            elif transformation == 'validate_cpf_cnpj':
                # Exemplo de limpeza de CPF/CNPJ
                cpf_cnpj_columns = ['cnpj_cpf_socio', 'representante_legal', 'cnpj_basico']
                for col in cpf_cnpj_columns:
                    if col in df.columns:
                        df = df.with_columns([
                            pl.col(col).str.replace_all(r'[^\d]', '').alias(col)
                        ])
            
            return df
            
        except Exception as e:
            self.logger.warning(f"Erro ao aplicar transformação '{transformation}': {str(e)}")
            return df
    
    def process_data_file(self, data_path: str) -> Optional[pl.DataFrame]:
        """
        Processa um único arquivo de dados (CSV).
        
        Usa configurações da entidade para determinar colunas e tipos.
        
        Args:
            data_path: Caminho para o arquivo de dados
            
        Returns:
            DataFrame processado ou None em caso de erro
        """
        try:
            entity_class = self.get_entity_class()
            column_names = [f"column_{i+1}" for i in range(len(entity_class.get_column_names()))]
            
            # Verificar se é arquivo de texto
            if not self._is_text_file(data_path):
                self.logger.warning(f"Arquivo {os.path.basename(data_path)} não é um arquivo de texto")
                return None
            
            # Tentar diferentes separadores
            separators = [config.file.separator, ';', ',', '|', '\t']
            
            for sep in separators:
                try:
                    df = pl.read_csv(
                        data_path,
                        separator=sep,
                        encoding=config.file.encoding,
                        has_header=False,
                        new_columns=column_names,
                        infer_schema_length=0,
                        dtypes={col: pl.Utf8 for col in column_names},
                        ignore_errors=True,
                        quote_char=None,
                        null_values=["", "NULL", "null", "00000000"],
                        missing_utf8_is_empty_string=True,
                        try_parse_dates=False,
                        truncate_ragged_lines=True
                    )
                    
                    if not df.is_empty():
                        self.logger.debug(f"Arquivo {os.path.basename(data_path)} processado com separador '{sep}'")
                        return df
                        
                except Exception as e:
                    self.logger.debug(f"Falha com separador '{sep}': {str(e)}")
                    continue
            
            self.logger.error(f"Não foi possível processar {os.path.basename(data_path)} com nenhum separador")
            return None
            
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivo {os.path.basename(data_path)}: {str(e)}")
            return None
    
    def _is_text_file(self, file_path: str) -> bool:
        """Verifica se o arquivo é um arquivo de texto."""
        try:
            with open(file_path, 'rb') as f:
                sample = f.read(4096)
                # Verificar se há muitos caracteres nulos ou bytes não-ASCII
                if b'\x00' in sample or len([b for b in sample if b > 127]) > len(sample) * 0.3:
                    return False
            return True
        except:
            return False
    
    def create_parquet_output(
        self, 
        df: pl.DataFrame, 
        output_path: str, 
        zip_prefix: str,
        partition_size: int = 500_000
    ) -> bool:
        """
        Salva DataFrame como Parquet com particionamento automático.
        
        Args:
            df: DataFrame a ser salvo
            output_path: Caminho de saída
            zip_prefix: Prefixo do arquivo ZIP original
            partition_size: Tamanho máximo de cada partição
            
        Returns:
            bool: True se sucesso, False caso contrário
        """
        try:
            if df.is_empty():
                self.logger.warning("DataFrame vazio, não salvando arquivo")
                return False
            
            # Garantir que o diretório existe
            os.makedirs(output_path, exist_ok=True)
            
            # Se o DataFrame é pequeno, salvar como arquivo único
            if df.height <= partition_size:
                filename = f"{zip_prefix}_{self.get_processor_name().lower()}.parquet"
                file_path = os.path.join(output_path, filename)
                
                df.write_parquet(file_path, compression='snappy')
                self.logger.info(f"Arquivo Parquet salvo: {filename} ({df.height} linhas)")
                return True
            
            # Para DataFrames grandes, particionar
            num_partitions = (df.height + partition_size - 1) // partition_size
            
            for i in range(num_partitions):
                start_idx = i * partition_size
                end_idx = min((i + 1) * partition_size, df.height)
                
                partition_df = df.slice(start_idx, end_idx - start_idx)
                filename = f"{zip_prefix}_{self.get_processor_name().lower()}_part_{i+1:03d}.parquet"
                file_path = os.path.join(output_path, filename)
                
                partition_df.write_parquet(file_path, compression='snappy')
                self.logger.debug(f"Partição salva: {filename} ({partition_df.height} linhas)")
            
            self.logger.info(f"DataFrame particionado salvo: {num_partitions} arquivos ({df.height} linhas total)")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao salvar Parquet: {str(e)}")
            return False
    
    # Interface pública para processamento
    
    def add_file_to_queue(self, zip_file: str, priority: int = 1, **extra_options) -> None:
        """
        Adiciona um arquivo à fila de processamento.
        
        Args:
            zip_file: Nome do arquivo ZIP
            priority: Prioridade do processamento
            **extra_options: Opções específicas para este arquivo
        """
        merged_options = {**self.options, **extra_options}
        self.queue_manager.add_to_queue(zip_file, priority, **merged_options)
    
    def start_processing(self, num_workers: int = None) -> None:
        """
        Inicia o processamento da fila.
        
        Args:
            num_workers: Número de workers (None para usar padrão)
        """
        if num_workers is None:
            num_workers = self.queue_manager.max_processes
        
        self.queue_manager.start_multiple_workers(
            num_workers=num_workers,
            process_function=self.process_single_zip_impl,
            path_zip=self.path_zip,
            path_unzip=self.path_unzip,
            path_parquet=self.path_parquet
        )
    
    def wait_for_completion(self) -> None:
        """Aguarda conclusão de todo o processamento."""
        self.queue_manager.wait_for_completion()
    
    def stop_processing(self) -> None:
        """Para todo o processamento."""
        self.queue_manager.stop_all_workers()
    
    def get_status(self) -> Dict[str, Any]:
        """Retorna status atual do processamento."""
        return self.queue_manager.get_status()
    
    def process_all_files(self) -> bool:
        """
        Processa todos os arquivos ZIP no diretório.
        
        Returns:
            bool: True se todos processados com sucesso
        """
        try:
            # Listar arquivos ZIP
            zip_files = [f for f in os.listdir(self.path_zip) if f.endswith('.zip')]
            
            if not zip_files:
                self.logger.warning("Nenhum arquivo ZIP encontrado")
                return True
            
            self.logger.info(f"Encontrados {len(zip_files)} arquivos ZIP para processar")
            
            # Adicionar todos à fila
            for zip_file in zip_files:
                self.add_file_to_queue(zip_file)
            
            # Iniciar processamento
            self.start_processing()
            
            # Aguardar conclusão
            self.wait_for_completion()
            
            self.logger.info("Processamento de todos os arquivos concluído")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro no processamento geral: {str(e)}")
            return False

    def process_single_zip(self, zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, **kwargs) -> bool:
        """
        Processa um único ZIP.
        
        Args:
            zip_file: Nome do arquivo ZIP
            path_zip: Diretório com arquivos ZIP
            path_unzip: Diretório para extração
            path_parquet: Diretório de saída
            **kwargs: Opções adicionais para o processamento
            
        Returns:
            bool: True se processado com sucesso, False caso contrário
        """
        try:
            # Extrair o ZIP
            with zipfile.ZipFile(os.path.join(path_zip, zip_file), 'r') as zip_ref:
                zip_ref.extractall(path_unzip)
            
            # Processar o conteúdo extraído
            df = self.process_single_zip_impl(
                zip_file,
                path_zip,
                path_unzip,
                path_parquet,
                **kwargs
            )
            
            if df is None:
                self.logger.error(f"Erro ao processar {zip_file}")
                return False
            
            # Salvar o resultado no formato Parquet
            if not self.create_parquet_output(df, path_parquet, zip_file):
                self.logger.error(f"Erro ao salvar {zip_file}")
                return False
            
            # Deletar o ZIP após processamento
            if self.delete_zips_after_extract:
                delete_zip_after_extraction(os.path.join(path_zip, zip_file))
            
            self.logger.info(f"Processado {zip_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao processar {zip_file}: {str(e)}")
            return False 