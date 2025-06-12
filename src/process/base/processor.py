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

# Import do Circuit Breaker Global
from ...utils.global_circuit_breaker import (
    circuit_breaker, 
    FailureType, 
    CriticalityLevel,
    should_continue_processing,
    report_critical_failure,
    report_fatal_failure
)

# Import condicional da config
try:
    from ...config import config
except ImportError:
    # Criar config básica se não existir
    class Config:
        class file:
            separator = ';'
            encoding = 'utf8-lossy'
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
        
        # Registrar callback de parada no circuit breaker
        circuit_breaker.register_stop_callback(self._emergency_stop)
        
        # Validar opções
        self._validate_options()
        
        # Verificar recursos críticos do sistema
        if not self._check_system_resources():
            report_fatal_failure(
                FailureType.SYSTEM_RESOURCE,
                "Recursos do sistema insuficientes para inicialização",
                self.get_processor_name()
            )
            return
        
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
        """
        Implementação específica do processamento de um único arquivo ZIP.
        
        Args:
            zip_file: Nome do arquivo ZIP
            path_zip: Diretório com arquivos ZIP
            path_unzip: Diretório para extração
            path_parquet: Diretório de saída
            **kwargs: Opções adicionais específicas do processador
            
        Returns:
            bool: True se processamento foi bem-sucedido, False caso contrário
        """
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
        df_columns = df.columns
        
        # Determinar tipo de entidade e aplicar mapeamento específico
        entity_name = entity_class.__name__ if hasattr(entity_class, '__name__') else str(entity_class)
        
        if entity_name == 'Estabelecimento':
            # Mapeamento para estabelecimentos (30 campos do CSV)
            mapping_dict = {
                'column_1': 'cnpj_basico',           # posição 1
                'column_2': 'cnpj_ordem',            # posição 2 (ordem)
                'column_3': 'cnpj_dv',               # posição 3 (dígitos verificadores)
                'column_4': 'matriz_filial',         # posição 4  
                'column_5': 'nome_fantasia',         # posição 5
                'column_6': 'codigo_situacao',       # posição 6
                'column_7': 'data_situacao_cadastral', # posição 7
                'column_8': 'codigo_motivo',         # posição 8
                'column_9': 'nome_cidade_exterior',  # posição 9
                'column_11': 'data_inicio_atividades', # posição 11
                'column_12': 'codigo_cnae',          # posição 12
                'column_13': 'cnae_secundaria',      # posição 13
                'column_20': 'uf',                   # posição 20
                'column_21': 'codigo_municipio',     # posição 21
                'column_19': 'cep',                  # posição 19
            }
        
        elif entity_name == 'Empresa':
            # Mapeamento para empresas (6 campos do CSV)
            mapping_dict = {
                'column_1': 'cnpj_basico',           # posição 1
                'column_2': 'razao_social',          # posição 2
                'column_3': 'natureza_juridica',     # posição 3
                'column_4': 'qualificacao_responsavel', # posição 4
                'column_5': 'capital_social',        # posição 5
                'column_6': 'porte_empresa',         # posição 6
                'column_7': 'ente_federativo_responsavel' # posição 7
            }
        
        elif entity_name == 'Simples':
            # Mapeamento para Simples Nacional (7 campos do CSV)
            mapping_dict = {
                'column_1': 'cnpj_basico',           # posição 1
                'column_2': 'opcao_simples',         # posição 2
                'column_3': 'data_opcao_simples',    # posição 3
                'column_4': 'data_exclusao_simples', # posição 4
                'column_5': 'opcao_mei',             # posição 5
                'column_6': 'data_opcao_mei',        # posição 6
                'column_7': 'data_exclusao_mei'      # posição 7
            }
        
        elif entity_name == 'Socio':
            # Mapeamento para sócios (11 campos do CSV)
            mapping_dict = {
                'column_1': 'cnpj_basico',           # posição 1
                'column_2': 'identificador_socio',   # posição 2
                'column_3': 'nome_socio',            # posição 3
                'column_4': 'cnpj_cpf_socio',        # posição 4
                'column_5': 'qualificacao_socio',    # posição 5
                'column_6': 'data_entrada_sociedade', # posição 6
                'column_7': 'cpf_representante_legal', # posição 7
                'column_8': 'nome_representante_legal', # posição 8
                'column_9': 'qualificacao_representante_legal', # posição 9
                'column_10': 'faixa_etaria',         # posição 10
                'column_11': 'pais'                  # posição 11
            }
        
        else:
            # Para entidades não reconhecidas, tentar mapeamento automático
            self.logger.warning(f"Entidade não reconhecida: {entity_name}. Usando mapeamento automático.")
            all_columns = entity_class.get_column_names()
            mapping_dict = {}
            for i, col_name in enumerate(all_columns, 1):
                df_col = f'column_{i}'
                if df_col in df_columns:
                    mapping_dict[df_col] = col_name
        
        # Filtrar apenas colunas que existem no DataFrame
        mapping = {}
        for df_col, entity_col in mapping_dict.items():
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
            for col_name, target_type in type_mapping.items():
                if col_name in df.columns:
                    current_type = df[col_name].dtype
                    
                    # Só converter se o tipo atual for diferente do tipo alvo
                    if current_type != target_type:
                        # Para conversões de Int64 -> Int32, fazer conversão direta
                        if current_type == pl.Int64 and target_type == pl.Int32:
                            conversions.append(
                                pl.col(col_name).cast(target_type, strict=False).alias(col_name)
                            )
                        # Para conversões de string para numérico, tentar limpeza se necessário
                        elif current_type == pl.Utf8 and target_type in [pl.Int32, pl.Int64, pl.Float64]:
                            conversions.append(
                                pl.col(col_name)
                                .str.strip_chars('"')  # Remove aspas duplas se houver
                                .str.strip_chars()      # Remove espaços
                                .cast(target_type, strict=False)
                                .alias(col_name)
                            )
                        # Para outras conversões, fazer conversão simples
                        else:
                            conversions.append(
                                pl.col(col_name).cast(target_type, strict=False).alias(col_name)
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
            if transformation == 'extract_cpf':
                # Extrair CPF da razão social para empresas
                if 'razao_social' in df.columns:
                    df = df.with_columns([
                        pl.col('razao_social').str.extract(r'(\d{11})', 1).alias('cpf_extraido')
                    ])
                    self.logger.debug("CPF extraído da razão social")
            
            elif transformation == 'clean_razao_social':
                # Limpar razão social removendo CPF
                if 'razao_social' in df.columns:
                    df = df.with_columns([
                        pl.col('razao_social').str.replace_all(r'\d{11}', '').str.strip_chars().alias('razao_social')
                    ])
                    self.logger.debug("Razão social limpa (CPF removido)")
            
            elif transformation == 'convert_capital_social':
                # Converter capital social para float
                if 'capital_social' in df.columns:
                    df = df.with_columns([
                        pl.col('capital_social').cast(pl.Utf8).str.replace_all(r'[^\d.,]', '').str.replace(',', '.').cast(pl.Float64, strict=False).alias('capital_social')
                    ])
            
            elif transformation == 'normalize_strings':
                # Normalizar strings
                string_fields = ['razao_social', 'ente_federativo_responsavel']
                for field in string_fields:
                    if field in df.columns:
                        df = df.with_columns([
                            pl.col(field).str.strip_chars().str.to_uppercase().alias(field)
                        ])
            
            elif transformation == 'validate_cpf_cnpj':
                # Limpeza de CPF/CNPJ
                cpf_cnpj_columns = ['cnpj_cpf_socio', 'representante_legal', 'cnpj_basico']
                for col in cpf_cnpj_columns:
                    if col in df.columns:
                        # Verificar tipo da coluna antes de aplicar transformação de string
                        col_dtype = df[col].dtype
                        if col_dtype == pl.Utf8:  # Só aplicar se for string
                            df = df.with_columns([
                                pl.col(col).str.replace_all(r'[^\d]', '').alias(col)
                            ])
                        elif col_dtype in [pl.Int64, pl.Int32, pl.Int8, pl.UInt64, pl.UInt32, pl.UInt8]:
                            # Se já é inteiro, converter para string, limpar e voltar para inteiro
                            df = df.with_columns([
                                pl.col(col)
                                .cast(pl.Utf8, strict=False)
                                .str.replace_all(r'[^\d]', '')
                                .cast(pl.Int64, strict=False)
                                .alias(col)
                            ])
                        # Se for outro tipo, manter como está
            
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
            all_columns = entity_class.get_column_names()
            
            # Para empresas, o cpf_extraido é calculado, não vem do CSV
            if hasattr(entity_class, '__name__') and entity_class.__name__ == 'Empresa':
                csv_columns = [col for col in all_columns if col != 'cpf_extraido']
            else:
                csv_columns = all_columns
            
            column_names = [f"column_{i+1}" for i in range(len(csv_columns))]
            
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
                        infer_schema_length=0,  # ALTERADO: Não inferir schema automaticamente
                        ignore_errors=True,
                        quote_char='"',  # Definir aspas duplas como caractere de citação
                        null_values=["", "NULL", "null", "00000000"],
                        missing_utf8_is_empty_string=True,
                        try_parse_dates=False,  # IMPORTANTE: Não fazer parse automático de datas
                        truncate_ragged_lines=True,
                        schema={col: pl.Utf8 for col in column_names}  # CORRIGIDO: Usar schema em vez de dtypes
                    )
                    
                    if isinstance(df, pl.DataFrame) and not df.is_empty():
                        self.logger.debug(f"Arquivo {os.path.basename(data_path)} processado com separador '{sep}' - todas colunas como string")
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
        Salva DataFrame como Parquet com particionamento automático e organização por tipo.
        
        Args:
            df: DataFrame a ser salvo
            output_path: Caminho de saída base
            zip_prefix: Prefixo do arquivo ZIP original
            partition_size: Tamanho máximo de cada partição
            
        Returns:
            bool: True se sucesso, False caso contrário
        """
        try:
            if not isinstance(df, pl.DataFrame):
                self.logger.error("Objeto passado não é um DataFrame válido")
                return False
                
            if df.is_empty():
                self.logger.warning("DataFrame vazio, não salvando arquivo")
                return False
            
            # Criar subpasta por tipo de entidade
            entity_folder = self.get_processor_name().lower()
            organized_output_path = os.path.join(output_path, entity_folder)
            
            # Garantir que o diretório existe
            os.makedirs(organized_output_path, exist_ok=True)
            
            self.logger.info(f"Salvando arquivos em: {organized_output_path}")
            
            # Se o DataFrame é pequeno, salvar como arquivo único
            if df.height <= partition_size:
                filename = f"{zip_prefix}.parquet"
                file_path = os.path.join(organized_output_path, filename)
                
                df.write_parquet(file_path, compression='snappy')
                self.logger.info(f"Arquivo Parquet salvo: {entity_folder}/{filename} ({df.height} linhas)")
                return True
            
            # Para DataFrames grandes, particionar
            num_partitions = (df.height + partition_size - 1) // partition_size
            
            for i in range(num_partitions):
                start_idx = i * partition_size
                end_idx = min((i + 1) * partition_size, df.height)
                
                partition_df = df.slice(start_idx, end_idx - start_idx)
                filename = f"{zip_prefix}_part_{i+1:03d}.parquet"
                file_path = os.path.join(organized_output_path, filename)
                
                partition_df.write_parquet(file_path, compression='snappy')
                self.logger.debug(f"Partição salva: {entity_folder}/{filename} ({partition_df.height} linhas)")
            
            self.logger.info(f"DataFrame particionado salvo: {num_partitions} arquivos em {entity_folder}/ ({df.height} linhas total)")
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
            # Verificar se aplicação deve continuar
            if not should_continue_processing():
                self.logger.warning(f"🛑 Processamento interrompido pelo circuit breaker: {zip_file}")
                return False
            
            # Chamar a implementação específica do processador
            success = self.process_single_zip_impl(
                zip_file,
                path_zip,
                path_unzip,
                path_parquet,
                **kwargs
            )
            
            if not success:
                self.logger.error(f"Erro ao processar {zip_file}")
                
                # Reportar falha de processamento
                circuit_breaker.report_failure(
                    FailureType.PROCESSING_FAILURE,
                    f"Falha no processamento do arquivo {zip_file}",
                    self.get_processor_name(),
                    CriticalityLevel.MODERATE,
                    {'zip_file': zip_file, 'paths': {'zip': path_zip, 'unzip': path_unzip, 'parquet': path_parquet}}
                )
                
                return False
            
            # Deletar o ZIP após processamento se solicitado
            if self.delete_zips_after_extract:
                zip_path = os.path.join(path_zip, zip_file)
                try:
                    os.remove(zip_path)
                    self.logger.info(f"ZIP deletado após processamento: {zip_file}")
                except Exception as e:
                    self.logger.warning(f"Erro ao deletar ZIP {zip_file}: {e}")
                    # Falha na limpeza não é crítica
                    circuit_breaker.report_failure(
                        FailureType.SYSTEM_RESOURCE,
                        f"Erro ao deletar ZIP {zip_file}: {e}",
                        self.get_processor_name(),
                        CriticalityLevel.WARNING,
                        {'zip_file': zip_file, 'zip_path': zip_path}
                    )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao processar {zip_file}: {str(e)}")
            
            # Reportar erro inesperado como falha crítica
            circuit_breaker.report_failure(
                FailureType.PROCESSING_FAILURE,
                f"Erro inesperado ao processar {zip_file}: {str(e)}",
                self.get_processor_name(),
                CriticalityLevel.MODERATE,
                {'zip_file': zip_file, 'error': str(e), 'error_type': type(e).__name__}
            )
            
            return False

    def _emergency_stop(self) -> None:
        """
        Callback de parada de emergência acionado pelo circuit breaker.
        """
        try:
            self.logger.critical("🚨 PARADA DE EMERGÊNCIA ACIONADA!")
            
            # Parar processamento da fila
            if hasattr(self, 'queue_manager') and self.queue_manager:
                self.queue_manager.stop_all_workers()
                self.logger.info("⏹️  Workers de processamento interrompidos")
            
            # Sinalizar parada para outros componentes
            self.logger.info("🔴 Processador em modo de parada de emergência")
            
        except Exception as e:
            self.logger.error(f"❌ Erro durante parada de emergência: {e}")
    
    def _check_system_resources(self) -> bool:
        """
        Verifica se há recursos suficientes do sistema para operação.
        
        Returns:
            bool: True se recursos suficientes, False caso contrário
        """
        try:
            import psutil
            
            # Verificar espaço em disco (pelo menos 1GB livre)
            try:
                disk_usage = psutil.disk_usage(self.path_parquet)
                free_gb = disk_usage.free / (1024**3)
                
                if free_gb < 1.0:
                    self.logger.error(f"💾 Espaço insuficiente em disco: {free_gb:.2f} GB livre")
                    report_critical_failure(
                        FailureType.DISK_SPACE,
                        f"Apenas {free_gb:.2f} GB livres em {self.path_parquet}",
                        self.get_processor_name(),
                        {'free_gb': free_gb, 'path': self.path_parquet}
                    )
                    return False
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Não foi possível verificar espaço em disco: {e}")
            
            # Verificar memória (pelo menos 1GB disponível)
            try:
                memory = psutil.virtual_memory()
                available_gb = memory.available / (1024**3)
                
                if available_gb < 1.0:
                    self.logger.error(f"🧠 Memória insuficiente: {available_gb:.2f} GB disponível")
                    report_critical_failure(
                        FailureType.MEMORY,
                        f"Apenas {available_gb:.2f} GB de memória disponível",
                        self.get_processor_name(),
                        {'available_gb': available_gb, 'total_gb': memory.total / (1024**3)}
                    )
                    return False
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Não foi possível verificar memória: {e}")
            
            # Verificar permissões de escrita
            test_paths = [self.path_zip, self.path_unzip, self.path_parquet]
            for path in test_paths:
                try:
                    os.makedirs(path, exist_ok=True)
                    
                    # Tentar criar arquivo de teste
                    test_file = os.path.join(path, '.test_permissions')
                    with open(test_file, 'w') as f:
                        f.write("test")
                    os.remove(test_file)
                    
                except Exception as e:
                    self.logger.error(f"📂 Erro de permissão em {path}: {e}")
                    report_critical_failure(
                        FailureType.PERMISSIONS,
                        f"Sem permissão de escrita em {path}",
                        self.get_processor_name(),
                        {'path': path, 'error': str(e)}
                    )
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao verificar recursos do sistema: {e}")
            return False 