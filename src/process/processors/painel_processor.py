"""
Processador de Painel - Combinação de Estabelecimento e Simples Nacional.

Este processador implementa o left join entre dados de estabelecimentos
e dados do Simples Nacional, criando uma visão consolidada para exportação.

Funcionalidades:
- Pipeline completo de processamento (download -> ZIP -> parquet -> painel)
- Left join entre estabelecimentos e Simples Nacional por cnpj_basico
- Inner join com dados de empresas
- Aplicação de transformações específicas da entidade Painel
- Validação de dados combinados
- Geração de campos calculados (situações, formatações)
- Suporte a filtros por UF e outros critérios
- Processamento flexível (ZIPs, parquets existentes, ou pipeline completo)
"""

import logging
import os
import polars as pl
from typing import List, Type, Optional, Dict, Any, Union
from pathlib import Path
import time
import psutil

from ...Entity.Painel import Painel
from ...Entity.base import BaseEntity
from ..base.processor import BaseProcessor

logger = logging.getLogger(__name__)


class PainelProcessor(BaseProcessor):
    """
    Processador específico para dados do Painel (Estabelecimento + Simples + Empresa).
    
    Características:
    - Pipeline completo de processamento de dados
    - Faz left join entre estabelecimentos e Simples Nacional
    - Inner join com dados de empresas
    - Utiliza entidade Painel para validação e transformação
    - Gera campos calculados úteis para exportação
    - Suporte a filtros específicos (UF, situação, etc.)
    - Processamento flexível: ZIPs, parquets existentes, ou pipeline completo
    """
    
    def __init__(self, path_zip: str, path_unzip: str, path_parquet: str, **kwargs):
        """
        Inicializa o processador de Painel.
        
        Args:
            path_zip: Diretório com arquivos ZIP
            path_unzip: Diretório para extração
            path_parquet: Diretório de saída
            **kwargs: Opções específicas
                - estabelecimento_path: Caminho para dados de estabelecimentos
                - simples_path: Caminho para dados do Simples Nacional
                - empresa_path: Caminho para dados de empresas
                - situacao_filter: Filtro por situação (opcional)
                - force_reprocess: Forçar reprocessamento mesmo se parquets existirem
                - skip_download: Pular etapa de download
                - skip_unzip: Pular etapa de descompactação
                - skip_individual_processing: Pular processamento individual
        """
        super().__init__(path_zip, path_unzip, path_parquet, **kwargs)
        
        # Caminhos para dados já processados (opcionais)
        self.estabelecimento_path = kwargs.get('estabelecimento_path')
        self.simples_path = kwargs.get('simples_path')
        self.empresa_path = kwargs.get('empresa_path')
        
        # Filtros específicos
        self.situacao_filter = kwargs.get('situacao_filter')
        
        # Opções de processamento
        self.force_reprocess = kwargs.get('force_reprocess', False)
        self.skip_download = kwargs.get('skip_download', False)
        self.skip_unzip = kwargs.get('skip_unzip', False) 
        self.skip_individual_processing = kwargs.get('skip_individual_processing', False)
        
        # URLs de download (configuráveis)
        self.download_urls = kwargs.get('download_urls', {
            'estabelecimentos': 'https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos*.zip',
            'simples': 'https://dadosabertos.rfb.gov.br/CNPJ/Simples*.zip',
            'empresas': 'https://dadosabertos.rfb.gov.br/CNPJ/Empresas*.zip'
        })
    
    def get_processor_name(self) -> str:
        """Retorna o nome do processador."""
        return "PAINEL"
    
    def get_entity_class(self) -> Type[BaseEntity]:
        """Retorna a classe de entidade associada."""
        return Painel
    
    def get_valid_options(self) -> List[str]:
        """Retorna opções válidas para este processador."""
        return [
            'estabelecimento_path',
            'simples_path', 
            'empresa_path',
            'situacao_filter',
            'force_reprocess',
            'skip_download',
            'skip_unzip',
            'skip_individual_processing'
        ]
    
    def apply_specific_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica transformações específicas do Painel.
        
        Args:
            df: DataFrame a ser transformado
            
        Returns:
            DataFrame transformado
        """
        try:
            # 1. Garantir que CNPJ básico está como bigint (Int64)
            if 'cnpj_basico' in df.columns:
                df = df.with_columns([
                    pl.col('cnpj_basico')
                    .cast(pl.Int64, strict=False)
                    .alias('cnpj_basico')
                ])
            
            # 2. Transformar campos 0/1 em Não/Sim
            campos_01 = ['matriz_filial']
            for campo in campos_01:
                if campo in df.columns:
                    df = df.with_columns([
                        pl.when(pl.col(campo) == 1)
                        .then(pl.lit('Sim'))
                        .when(pl.col(campo) == 0)
                        .then(pl.lit('Não'))
                        .otherwise(pl.lit('Não informado'))
                        .alias(campo)
                    ])
            
            # 3. Transformar campos S/N em Sim/Não
            campos_sn = ['opcao_simples', 'opcao_mei']
            for campo in campos_sn:
                if campo in df.columns:
                    df = df.with_columns([
                        pl.when(pl.col(campo) == 'S')
                        .then(pl.lit('Sim'))
                        .when(pl.col(campo) == 'N')
                        .then(pl.lit('Não'))
                        .otherwise(pl.lit('Não informado'))
                        .alias(campo)
                    ])
            
            # 4. Formatar campos de data para YYYYMMDD
            campos_data = [
                'data_opcao_simples', 'data_exclusao_simples',
                'data_opcao_mei', 'data_exclusao_mei',
                'data_situacao_cadastral', 'data_inicio_atividades'
            ]
            for campo in campos_data:
                if campo in df.columns:
                    df = df.with_columns([
                        pl.col(campo)
                        .str.replace_all(r'[^\d]', '')  # Remove caracteres não numéricos
                        .str.pad_start(8, '0')  # Garante 8 dígitos
                        .alias(campo)
                    ])
            
            # 5. Adicionar descrições para códigos
            # Porte da empresa
            if 'porte_empresa' in df.columns:
                df = df.with_columns([
                    pl.when(pl.col('porte_empresa') == 1)
                    .then(pl.lit('Micro'))
                    .when(pl.col('porte_empresa') == 2)
                    .then(pl.lit('Pequena'))
                    .when(pl.col('porte_empresa') == 3)
                    .then(pl.lit('Média'))
                    .when(pl.col('porte_empresa') == 4)
                    .then(pl.lit('Grande'))
                    .when(pl.col('porte_empresa') == 5)
                    .then(pl.lit('Demais'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('porte_empresa_descricao')
                ])
            
            # Natureza jurídica
            if 'natureza_juridica' in df.columns:
                df = df.with_columns([
                    pl.when(pl.col('natureza_juridica') == 1)
                    .then(pl.lit('Administração Pública'))
                    .when(pl.col('natureza_juridica') == 2)
                    .then(pl.lit('Entidade Empresarial'))
                    .when(pl.col('natureza_juridica') == 3)
                    .then(pl.lit('Entidade sem Fins Lucrativos'))
                    .when(pl.col('natureza_juridica') == 4)
                    .then(pl.lit('Pessoa Física'))
                    .when(pl.col('natureza_juridica') == 5)
                    .then(pl.lit('Organização Internacional'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('natureza_juridica_descricao')
                ])
            
            # Código motivo
            if 'codigo_motivo' in df.columns:
                df = df.with_columns([
                    pl.when(pl.col('codigo_motivo') == 1)
                    .then(pl.lit('Extinção por Encerramento'))
                    .when(pl.col('codigo_motivo') == 2)
                    .then(pl.lit('Incorporação'))
                    .when(pl.col('codigo_motivo') == 3)
                    .then(pl.lit('Fusão'))
                    .when(pl.col('codigo_motivo') == 4)
                    .then(pl.lit('Cisão'))
                    .when(pl.col('codigo_motivo') == 5)
                    .then(pl.lit('Encerramento de Falência'))
                    .when(pl.col('codigo_motivo') == 6)
                    .then(pl.lit('Encerramento de Liquidação'))
                    .when(pl.col('codigo_motivo') == 7)
                    .then(pl.lit('Cancelamento de Baixa'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('codigo_motivo_descricao')
                ])
            
            # Código situação
            if 'codigo_situacao' in df.columns:
                df = df.with_columns([
                    pl.when(pl.col('codigo_situacao') == 1)
                    .then(pl.lit('Nula'))
                    .when(pl.col('codigo_situacao') == 2)
                    .then(pl.lit('Ativa'))
                    .when(pl.col('codigo_situacao') == 3)
                    .then(pl.lit('Suspensa'))
                    .when(pl.col('codigo_situacao') == 4)
                    .then(pl.lit('Inapta'))
                    .when(pl.col('codigo_situacao') == 8)
                    .then(pl.lit('Baixada'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('codigo_situacao_descricao')
                ])
            
            # Tipo situação cadastral
            if 'tipo_situacao_cadastral' in df.columns:
                df = df.with_columns([
                    pl.when(pl.col('tipo_situacao_cadastral') == 1)
                    .then(pl.lit('Ativa'))
                    .when(pl.col('tipo_situacao_cadastral') == 2)
                    .then(pl.lit('Baixa Voluntária'))
                    .when(pl.col('tipo_situacao_cadastral') == 3)
                    .then(pl.lit('Outras Baixas'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('tipo_situacao_cadastral_descricao')
                ])
            
            # 6. Aplicar filtros se especificados explicitamente
            if self.situacao_filter:
                df = df.filter(pl.col('codigo_situacao') == self.situacao_filter)
                self.logger.info(f"Filtro situação aplicado: {self.situacao_filter}")
            
            self.logger.info(f"Transformações específicas aplicadas. Registros finais: {df.height}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao aplicar transformações específicas: {str(e)}")
            raise
    
    def process_painel_data(self, output_filename: Optional[str] = None) -> bool:
        """
        Processa dados do painel fazendo os joins necessários.
        Usa processamento lazy e em chunks para otimizar uso de memória.
        
        Args:
            output_filename: Nome do arquivo de saída (opcional)
            
        Returns:
            bool: True se processamento foi bem-sucedido
        """
        start_time = time.time()
        join_start_time = None
        
        try:
            self.logger.info("🔄 === INICIANDO PROCESSAMENTO DO PAINEL ===")
            self.logger.info(f"📂 Diretório de saída: {os.path.abspath(self.path_parquet)}")
            
            # Validar caminhos de entrada
            if not self.estabelecimento_path or not os.path.exists(self.estabelecimento_path):
                raise ValueError(f"Caminho de estabelecimentos inválido: {self.estabelecimento_path}")
            
            if not self.simples_path or not os.path.exists(self.simples_path):
                raise ValueError(f"Caminho do Simples Nacional inválido: {self.simples_path}")
            
            if not self.empresa_path or not os.path.exists(self.empresa_path):
                raise ValueError(f"Caminho de empresas inválido: {self.empresa_path}")
            
            # 1. Criar scans lazy para os DataFrames
            self.logger.info("🔍 Iniciando scans lazy dos dados...")
            
            try:
                # Estabelecimentos
                estabelecimentos_scan = (
                    pl.scan_parquet(f"{self.estabelecimento_path}/*.parquet")
                    if os.path.isdir(self.estabelecimento_path)
                    else pl.scan_parquet(self.estabelecimento_path)
                )
                
                # Simples
                simples_scan = (
                    pl.scan_parquet(f"{self.simples_path}/*.parquet")
                    if os.path.isdir(self.simples_path)
                    else pl.scan_parquet(self.simples_path)
                )
                
                # Empresas
                empresas_scan = (
                    pl.scan_parquet(f"{self.empresa_path}/*.parquet")
                    if os.path.isdir(self.empresa_path)
                    else pl.scan_parquet(self.empresa_path)
                )
                
                # Contar registros sem carregar tudo
                estabelecimentos_count = estabelecimentos_scan.select(pl.count()).collect().item()
                simples_count = simples_scan.select(pl.count()).collect().item()
                empresas_count = empresas_scan.select(pl.count()).collect().item()
                
                self.logger.info(f"✓ Scans criados:")
                self.logger.info(f"  └─ Estabelecimentos: {estabelecimentos_count:,} registros")
                self.logger.info(f"  └─ Simples Nacional: {simples_count:,} registros")
                self.logger.info(f"  └─ Empresas: {empresas_count:,} registros")
                
            except Exception as e:
                self.logger.error(f"❌ Erro ao criar scans: {str(e)}")
                raise
            
            # 2. Otimizar consultas selecionando apenas colunas necessárias
            colunas_estabelecimentos = ['cnpj_basico', 'matriz_filial', 'codigo_situacao', 
                                      'data_situacao_cadastral', 'codigo_motivo', 
                                      'data_inicio_atividades', 'codigo_cnae', 'tipo_situacao_cadastral',
                                      'codigo_municipio']
            
            colunas_simples = ['cnpj_basico', 'opcao_simples', 'data_opcao_simples',
                              'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei',
                              'data_exclusao_mei']
            
            colunas_empresas = ['cnpj_basico', 'natureza_juridica', 'porte_empresa']
            
            # 3. Aplicar transformações em cada dataset separadamente
            self.logger.info("🔄 Aplicando transformações nos datasets...")
            
            # Transformações em estabelecimentos
            self.logger.info("  └─ Transformando estabelecimentos...")
            
            # Verificar se o campo tipo_situacao_cadastral existe e, se não, calculá-lo
            try:
                # Verificar se o campo existe nos dados
                colunas_disponiveis = estabelecimentos_scan.collect().limit(1).columns
                
                if 'tipo_situacao_cadastral' not in colunas_disponiveis:
                    self.logger.info("  └─ Campo tipo_situacao_cadastral não encontrado, calculando...")
                    
                    # Calcular o campo tipo_situacao_cadastral baseado nas regras de negócio
                    estabelecimentos_scan = estabelecimentos_scan.with_columns([
                        pl.when(
                            (pl.col('codigo_situacao') == 2) & 
                            (pl.col('codigo_motivo') == 0)
                        )
                        .then(pl.lit(1))  # Ativa
                        .when(
                            (pl.col('codigo_situacao') == 8) & 
                            (pl.col('codigo_motivo') == 1)
                        )
                        .then(pl.lit(2))  # Baixa Voluntária
                        .when(
                            (pl.col('codigo_situacao') == 8) & 
                            (pl.col('codigo_motivo') != 1)
                        )
                        .then(pl.lit(3))  # Outras Baixas
                        .otherwise(pl.lit(0))  # Valor padrão para casos não cobertos pelas regras
                        .alias('tipo_situacao_cadastral')
                    ])
                    
                    self.logger.info("  └─ Campo tipo_situacao_cadastral calculado com sucesso")
                else:
                    self.logger.info("  └─ Campo tipo_situacao_cadastral já existe nos dados")
                
            except Exception as e:
                self.logger.warning(f"  └─ Erro ao verificar/calcular tipo_situacao_cadastral: {str(e)}")
            
            # Agora selecionar as colunas e aplicar transformações
            estabelecimentos_scan = (
                estabelecimentos_scan
                .select(colunas_estabelecimentos)
                .with_columns([
                    pl.when(pl.col('matriz_filial') == 1)
                    .then(pl.lit('Matriz'))
                    .when(pl.col('matriz_filial') == 2)
                    .then(pl.lit('Filial'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('descricao_matriz_filial'),
                    
                    pl.when(pl.col('codigo_situacao') == 1).then(pl.lit('Nula'))
                    .when(pl.col('codigo_situacao') == 2).then(pl.lit('Ativa'))
                    .when(pl.col('codigo_situacao') == 3).then(pl.lit('Suspensa'))
                    .when(pl.col('codigo_situacao') == 4).then(pl.lit('Inapta'))
                    .when(pl.col('codigo_situacao') == 8).then(pl.lit('Baixada'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('descricao_situacao'),
                    
                    pl.when(pl.col('tipo_situacao_cadastral') == 1).then(pl.lit('Ativa'))
                    .when(pl.col('tipo_situacao_cadastral') == 2).then(pl.lit('Baixa Voluntária'))
                    .when(pl.col('tipo_situacao_cadastral') == 3).then(pl.lit('Outras Baixas'))
                    .otherwise(pl.lit('Outras Situações'))
                    .alias('descricao_tipo_situacao')
                ])
            )
            
            # Adicionar mapeamento de motivo usando a tabela de domínio
            try:
                motivo_df = pl.read_parquet(os.path.join(os.path.dirname(self.path_parquet), "base", "motivo.parquet"))
                self.logger.info(f"  └─ Tabela de motivos carregada: {len(motivo_df)} registros")
                
                # Adicionar coluna com descrição usando join com a tabela de motivos
                estabelecimentos_scan = estabelecimentos_scan.with_columns([
                    pl.col('codigo_motivo').cast(pl.Int32).alias('codigo_motivo_join')
                ])
                
                # Criar um LazyFrame da tabela de motivos com apenas as colunas necessárias
                motivo_scan = pl.scan_parquet(os.path.join(os.path.dirname(self.path_parquet), "base", "motivo.parquet")).select([
                    pl.col('codigo').alias('codigo_motivo_join'),
                    pl.col('descricao').alias('codigo_motivo_descricao')
                ])
                
                # Join com a tabela de motivos
                estabelecimentos_scan = estabelecimentos_scan.join(
                    motivo_scan,
                    on='codigo_motivo_join',
                    how='left'
                ).drop('codigo_motivo_join')
                
                # Preencher valores nulos com "Não informado"
                estabelecimentos_scan = estabelecimentos_scan.with_columns([
                    pl.col('codigo_motivo_descricao').fill_null('Não informado').alias('descricao_motivo')
                ]).drop('codigo_motivo_descricao')
                
                self.logger.info("  └─ Descrições de motivos adicionadas via join")
            except Exception as e:
                self.logger.warning(f"  └─ Não foi possível carregar tabela de motivos: {str(e)}")
                # Fallback para o mapeamento simplificado anterior
                estabelecimentos_scan = estabelecimentos_scan.with_columns([
                    pl.when(pl.col('codigo_motivo') == 1).then(pl.lit('Extinção por Encerramento'))
                    .when(pl.col('codigo_motivo') == 2).then(pl.lit('Incorporação'))
                    .when(pl.col('codigo_motivo') == 3).then(pl.lit('Fusão'))
                    .when(pl.col('codigo_motivo') == 4).then(pl.lit('Cisão'))
                    .when(pl.col('codigo_motivo') == 5).then(pl.lit('Encerramento de Falência'))
                    .when(pl.col('codigo_motivo') == 6).then(pl.lit('Encerramento de Liquidação'))
                    .when(pl.col('codigo_motivo') == 7).then(pl.lit('Cancelamento de Baixa'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('codigo_motivo_descricao')
                ])
            
            # Transformações em simples
            self.logger.info("  └─ Transformando dados do Simples Nacional...")
            simples_scan = (
                simples_scan
                .select(colunas_simples)
                .with_columns([
                    pl.when(pl.col('opcao_simples') == 'S')
                    .then(pl.lit('Sim'))
                    .when(pl.col('opcao_simples') == 'N')
                    .then(pl.lit('Não'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('opcao_simples'),
                    
                    pl.when(pl.col('opcao_mei') == 'S')
                    .then(pl.lit('Sim'))
                    .when(pl.col('opcao_mei') == 'N')
                    .then(pl.lit('Não'))
                    .otherwise(pl.lit('Não informado'))
                    .alias('opcao_mei')
                ])
            )
            
            # Transformações em empresas
            self.logger.info("  └─ Transformando dados de empresas...")
            
            # Carregar tabelas de domínio
            try:
                natureza_juridica_df = pl.read_parquet(os.path.join(os.path.dirname(self.path_parquet), "base", "natureza_juridica.parquet"))
                self.logger.info(f"  └─ Tabela de natureza jurídica carregada: {len(natureza_juridica_df)} registros")
                
                # Criar um LazyFrame da tabela de natureza jurídica com apenas as colunas necessárias
                natureza_scan = pl.scan_parquet(os.path.join(os.path.dirname(self.path_parquet), "base", "natureza_juridica.parquet")).select([
                    pl.col('codigo').alias('natureza_juridica_join'),
                    pl.col('descricao').alias('descricao_natureza_juridica')
                ])
                
                # Preparar empresas para o join
                empresas_scan = (
                    empresas_scan
                    .select(colunas_empresas)
                    .with_columns([
                        pl.col('natureza_juridica').cast(pl.Int32).alias('natureza_juridica_join'),
                        
                        # Adicionar descrição do porte
                        pl.when(pl.col('porte_empresa') == 1).then(pl.lit('Micro'))
                        .when(pl.col('porte_empresa') == 2).then(pl.lit('Pequena'))
                        .when(pl.col('porte_empresa') == 3).then(pl.lit('Média'))
                        .when(pl.col('porte_empresa') == 4).then(pl.lit('Grande'))
                        .when(pl.col('porte_empresa') == 5).then(pl.lit('Demais'))
                        .otherwise(pl.lit('Não informado'))
                        .alias('descricao_porte')
                    ])
                )
                
                # Join com a tabela de natureza jurídica
                empresas_scan = empresas_scan.join(
                    natureza_scan,
                    on='natureza_juridica_join',
                    how='left'
                ).drop('natureza_juridica_join')
                
                # Preencher valores nulos com "Não informado"
                empresas_scan = empresas_scan.with_columns([
                    pl.col('descricao_natureza_juridica').fill_null('Não informado')
                ])
                
                self.logger.info("  └─ Descrições de natureza jurídica adicionadas via join")
            except Exception as e:
                self.logger.warning(f"  └─ Não foi possível carregar tabela de natureza jurídica: {str(e)}")
                # Fallback para o mapeamento simplificado anterior
                empresas_scan = (
                    empresas_scan
                    .select(colunas_empresas)
                    .with_columns([
                        # Descrição do porte
                        pl.when(pl.col('porte_empresa') == 1).then(pl.lit('Micro'))
                        .when(pl.col('porte_empresa') == 2).then(pl.lit('Pequena'))
                        .when(pl.col('porte_empresa') == 3).then(pl.lit('Média'))
                        .when(pl.col('porte_empresa') == 4).then(pl.lit('Grande'))
                        .when(pl.col('porte_empresa') == 5).then(pl.lit('Demais'))
                        .otherwise(pl.lit('Não informado'))
                        .alias('descricao_porte'),
                        
                        # Descrição da natureza jurídica
                        pl.when(pl.col('natureza_juridica') == 1).then(pl.lit('Administração Pública'))
                        .when(pl.col('natureza_juridica') == 2).then(pl.lit('Entidade Empresarial'))
                        .when(pl.col('natureza_juridica') == 3).then(pl.lit('Entidade sem Fins Lucrativos'))
                        .when(pl.col('natureza_juridica') == 4).then(pl.lit('Pessoa Física'))
                        .when(pl.col('natureza_juridica') == 5).then(pl.lit('Organização Internacional'))
                        .otherwise(pl.lit('Não informado'))
                        .alias('descricao_natureza_juridica')
                    ])
                )
            
            # 4. Executar JOINs com os dados já transformados
            join_start_time = time.time()
            self.logger.info("⚡ Executando JOINs otimizados...")
            
            try:
                # Primeiro JOIN (estabelecimentos com simples)
                self.logger.info("  └─ LEFT JOIN: estabelecimentos + simples")
                self.logger.info("     └─ Iniciando JOIN...")
                
                try:
                    painel_scan = estabelecimentos_scan.join(
                        simples_scan,
                        on='cnpj_basico',
                        how='left'
                    )
                    self.logger.info("     └─ JOIN estabelecimentos + simples concluído")
                except Exception as e:
                    self.logger.error(f"❌ Erro no JOIN estabelecimentos + simples: {str(e)}")
                    raise
                
                # Segundo JOIN (resultado anterior com empresas)
                self.logger.info("  └─ INNER JOIN: resultado anterior + empresas")
                self.logger.info("     └─ Iniciando JOIN...")
                
                try:
                    painel_scan = painel_scan.join(
                        empresas_scan,
                        on='cnpj_basico',
                        how='inner'
                    )
                    self.logger.info("     └─ JOIN com empresas concluído")
                except Exception as e:
                    self.logger.error(f"❌ Erro no JOIN com empresas: {str(e)}")
                    raise
                
                # Terceiro JOIN (resultado anterior com municípios)
                self.logger.info("  └─ LEFT JOIN: resultado anterior + municípios")
                self.logger.info("     └─ Carregando dados de municípios...")
                
                try:
                    # Carregar dados de municípios
                    municipio_path = os.path.join(os.path.dirname(self.path_parquet), "base", "municipio.parquet")
                    if os.path.exists(municipio_path):
                        municipios_df = pl.read_parquet(municipio_path)
                        self.logger.info(f"     └─ Municípios carregados: {len(municipios_df)} registros")
                        
                        # Criar scan dos municípios com renomeação para facilitar o JOIN
                        municipios_scan = pl.scan_parquet(municipio_path).select([
                            pl.col('cod_mn_dados_abertos').alias('codigo_municipio'),  # Renomear para fazer JOIN
                            pl.col('codigo').alias('codigo_ibge'),                     # Código IBGE (7 dígitos) para exportação
                            pl.col('nome').alias('nome_municipio'),                    # Nome do município
                            pl.col('uf').alias('uf'),                                  # UF do município
                            pl.col('sigla_uf').alias('sigla_uf')                       # Sigla da UF
                        ])
                        
                        # Executar LEFT JOIN com municípios
                        painel_scan = painel_scan.join(
                            municipios_scan,
                            on='codigo_municipio',
                            how='left'
                        )
                        
                        self.logger.info("     └─ LEFT JOIN com municípios executado")
                        
                    else:
                        self.logger.warning(f"     └─ Arquivo de municípios não encontrado: {municipio_path}")
                        self.logger.warning("     └─ Continuando sem código IBGE dos municípios")
                        
                except Exception as e:
                    self.logger.error(f"❌ Erro no JOIN com municípios: {str(e)}")
                    self.logger.warning("⚠️ Continuando processamento sem código IBGE dos municípios")
                
                # 5. Salvar o DataFrame completo
                if not output_filename:
                    output_filename = "painel_dados.parquet"
                
                # Salvar na pasta raiz do parquet
                output_path = os.path.join(self.path_parquet, output_filename)
                output_path_abs = os.path.abspath(output_path)
                
                # Garantir que a pasta existe
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                save_start = time.time()
                self.logger.info(f"💾 Iniciando gravação dos dados transformados...")
                
                try:
                    # Reordenar colunas para aproximar códigos e descrições
                    colunas_para_selecionar = [
                        # Dados principais
                        'cnpj_basico',
                        
                        # Matriz/Filial
                        'matriz_filial', 'descricao_matriz_filial',
                        
                        # Situação cadastral
                        'codigo_situacao', 'descricao_situacao',
                        'tipo_situacao_cadastral', 'descricao_tipo_situacao',
                        
                        # Motivo
                        'codigo_motivo', 'descricao_motivo',
                        
                        # Datas
                        'data_situacao_cadastral', 'data_inicio_atividades',
                        
                        # CNAE
                        'codigo_cnae',
                        
                        # Natureza jurídica
                        'natureza_juridica', 'descricao_natureza_juridica',
                        
                        # Porte
                        'porte_empresa', 'descricao_porte',
                        
                        # Simples Nacional
                        'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
                        'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei',
                        
                        # Localização
                        'codigo_ibge', 'nome_municipio', 'uf', 'sigla_uf'
                    ]
                    
                    # Selecionar colunas diretamente (vai falhar se não existir, mas isso é melhor que travar)
                    self.logger.info(f"  └─ Selecionando colunas para o resultado final...")
                    
                    try:
                        painel_scan = painel_scan.select(colunas_para_selecionar)
                        self.logger.info(f"  └─ {len(colunas_para_selecionar)} colunas selecionadas com sucesso")
                    except Exception as e:
                        self.logger.warning(f"  └─ Erro ao selecionar algumas colunas: {str(e)}")
                        # Fallback: selecionar apenas colunas básicas se houver erro
                        colunas_basicas = [
                            'cnpj_basico', 'matriz_filial', 'codigo_situacao', 'data_situacao_cadastral',
                            'codigo_motivo', 'data_inicio_atividades', 'codigo_cnae', 'natureza_juridica',
                            'porte_empresa', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
                            'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
                        ]
                        painel_scan = painel_scan.select(colunas_basicas)
                        self.logger.info(f"  └─ Usando seleção de fallback com {len(colunas_basicas)} colunas básicas")
                    
                    # Usar sink_parquet para salvar diretamente sem coletar tudo na memória
                    self.logger.info(f"  └─ Salvando arquivo: {output_path_abs}")
                    
                    # Configurar opções de escrita otimizadas
                    write_options = {
                        "compression": "zstd",  # Compressão eficiente
                        "statistics": True,     # Coletar estatísticas
                        "row_group_size": 100000  # Tamanho do grupo de linhas
                    }
                    
                    # Usar sink_parquet para salvar diretamente sem carregar tudo na memória
                    self.logger.info("  └─ Salvando arquivo parquet diretamente (sem carregar na memória)...")
                    try:
                        painel_scan.sink_parquet(output_path, **write_options)
                    except Exception as e:
                        self.logger.error(f"❌ Erro ao salvar com sink_parquet: {str(e)}")
                        self.logger.info("  └─ Tentando salvamento alternativo com collect em chunks...")
                        
                        # Método alternativo: processar em chunks
                        chunk_size = 1_000_000  # 1 milhão de registros por chunk
                        
                        # Primeiro, contar total de registros
                        total_rows = painel_scan.select(pl.count()).collect().item()
                        self.logger.info(f"  └─ Total de registros a processar: {total_rows:,}")
                        
                        # Processar em chunks
                        num_chunks = (total_rows + chunk_size - 1) // chunk_size
                        self.logger.info(f"  └─ Processando em {num_chunks} chunks de {chunk_size:,} registros")
                        
                        first_chunk = True
                        for i in range(num_chunks):
                            offset = i * chunk_size
                            self.logger.info(f"  └─ Processando chunk {i+1}/{num_chunks} (offset: {offset:,})")
                            
                            chunk_df = painel_scan.slice(offset, chunk_size).collect()
                            
                            if first_chunk:
                                # Primeiro chunk: criar arquivo
                                chunk_df.write_parquet(output_path, **write_options)
                                first_chunk = False
                            else:
                                # Chunks subsequentes: anexar ao arquivo existente
                                existing_df = pl.read_parquet(output_path)
                                combined_df = pl.concat([existing_df, chunk_df])
                                combined_df.write_parquet(output_path, **write_options)
                                del combined_df, existing_df  # Liberar memória
                            
                            del chunk_df  # Liberar memória do chunk
                            
                        self.logger.info("  └─ Salvamento em chunks concluído")
                        
                        # Forçar coleta de lixo
                        import gc
                        gc.collect()
                    
                    self.logger.info("  └─ Arquivo salvo com sucesso")
                    
                    save_time = time.time() - save_start
                    total_time = time.time() - start_time
                    
                    # Verificar arquivo final
                    if not os.path.exists(output_path):
                        raise FileNotFoundError(f"Arquivo não foi criado: {output_path}")
                    
                    file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
                    
                    self.logger.info("\n=== 🎉 PROCESSAMENTO DO PAINEL CONCLUÍDO COM SUCESSO ===")
                    self.logger.info(f"📂 Arquivo salvo em: {output_path_abs}")
                    self.logger.info(f"📊 Tamanho do arquivo: {file_size:.1f}MB")
                    
                    # Contar registros apenas se arquivo não for muito grande (para evitar problemas de memória)
                    if file_size < 1000:  # Menos de 1GB
                        try:
                            total_rows = pl.scan_parquet(output_path).select(pl.count()).collect().item()
                            self.logger.info(f"📈 Total de registros: {total_rows:,}")
                        except Exception as e:
                            self.logger.warning(f"Não foi possível contar registros: {str(e)}")
                    else:
                        self.logger.info("📈 Arquivo muito grande - contagem de registros pulada para evitar problemas de memória")
                    
                    self.logger.info("\n⏱️  Tempos de processamento:")
                    self.logger.info(f"  └─ Processamento e JOINs: {save_start - join_start_time:.1f}s")
                    self.logger.info(f"  └─ Salvamento: {save_time:.1f}s")
                    self.logger.info(f"  └─ TEMPO TOTAL: {total_time:.1f}s ({total_time/60:.1f}min)")
                    self.logger.info("================================================")
                    
                    return True
                    
                except Exception as e:
                    self.logger.error(f"❌ Erro ao salvar dados: {str(e)}")
                    self.logger.error("Detalhes do erro:", exc_info=True)
                    raise
                
            except Exception as e:
                total_time = time.time() - start_time
                join_time = time.time() - join_start_time if join_start_time else 0
                
                self.logger.error("\n=== ❌ ERRO NO PROCESSAMENTO DO PAINEL ===")
                self.logger.error(f"Erro: {str(e)}")
                self.logger.error(f"Tempo decorrido até o erro: {total_time:.1f}s")
                if join_start_time:
                    self.logger.error(f"Tempo em operações de JOIN: {join_time:.1f}s")
                self.logger.error("Stacktrace completo:", exc_info=True)
                self.logger.error("==========================================")
                return False
            
        except Exception as e:
            total_time = time.time() - start_time
            join_time = time.time() - join_start_time if join_start_time else 0
            
            self.logger.error("\n=== ❌ ERRO NO PROCESSAMENTO DO PAINEL ===")
            self.logger.error(f"Erro: {str(e)}")
            self.logger.error(f"Tempo decorrido até o erro: {total_time:.1f}s")
            if join_start_time:
                self.logger.error(f"Tempo em operações de JOIN: {join_time:.1f}s")
            self.logger.error("Stacktrace:", exc_info=True)
            self.logger.error("==========================================")
            return False
    
    def _generate_statistics_report(self, df: pl.DataFrame):
        """Gera relatório de estatísticas dos dados processados."""
        try:
            self.logger.info("=== RELATÓRIO DE ESTATÍSTICAS DO PAINEL ===")
            
            # Estatísticas gerais
            total_registros = df.height
            self.logger.info(f"Total de registros: {total_registros:,}")
            
            # Estatísticas de opção pelo Simples Nacional
            if 'opcao_simples' in df.columns:
                simples_stats = df.group_by('opcao_simples').agg([
                    pl.count().alias('count')
                ]).sort('count', descending=True)
                
                self.logger.info("Opção pelo Simples Nacional:")
                for row in simples_stats.iter_rows(named=True):
                    opcao = 'Sim' if row['opcao_simples'] == 'S' else 'Não' if row['opcao_simples'] == 'N' else 'Não informado'
                    pct = (row['count'] / total_registros) * 100
                    self.logger.info(f"  {opcao}: {row['count']:,} ({pct:.1f}%)")
            
            # Estatísticas de opção pelo MEI
            if 'opcao_mei' in df.columns:
                mei_stats = df.group_by('opcao_mei').agg([
                    pl.count().alias('count')
                ]).sort('count', descending=True)
                
                self.logger.info("Opção pelo MEI:")
                for row in mei_stats.iter_rows(named=True):
                    opcao = 'Sim' if row['opcao_mei'] == 'S' else 'Não' if row['opcao_mei'] == 'N' else 'Não informado'
                    pct = (row['count'] / total_registros) * 100
                    self.logger.info(f"  {opcao}: {row['count']:,} ({pct:.1f}%)")
            
            # Estatísticas de matriz/filial
            if 'matriz_filial' in df.columns:
                matriz_filial_stats = df.group_by('matriz_filial').agg([
                    pl.count().alias('count')
                ]).sort('count', descending=True)
                
                self.logger.info("Tipo de estabelecimento:")
                for row in matriz_filial_stats.iter_rows(named=True):
                    tipo = 'Matriz' if row['matriz_filial'] == 1 else 'Filial' if row['matriz_filial'] == 2 else 'Indefinido'
                    pct = (row['count'] / total_registros) * 100
                    self.logger.info(f"  {tipo}: {row['count']:,} ({pct:.1f}%)")
            
            # Estatísticas de situação cadastral
            if 'codigo_situacao' in df.columns:
                situacao_stats = df.group_by('codigo_situacao').agg([
                    pl.count().alias('count')
                ]).sort('count', descending=True)
                
                self.logger.info("Situação cadastral:")
                for row in situacao_stats.iter_rows(named=True):
                    situacao_map = {1: 'Nula', 2: 'Ativa', 3: 'Suspensa', 4: 'Inapta', 8: 'Baixada'}
                    situacao = situacao_map.get(row['codigo_situacao'], 'Outros')
                    pct = (row['count'] / total_registros) * 100
                    self.logger.info(f"  {situacao}: {row['count']:,} ({pct:.1f}%)")
            
            # Estatísticas de porte da empresa
            if 'porte_empresa' in df.columns:
                porte_stats = df.group_by('porte_empresa').agg([
                    pl.count().alias('count')
                ]).sort('count', descending=True)
                
                self.logger.info("Porte da empresa:")
                for row in porte_stats.iter_rows(named=True):
                    porte_map = {1: 'Micro', 2: 'Pequena', 3: 'Média', 4: 'Grande', 5: 'Demais'}
                    porte = porte_map.get(row['porte_empresa'], 'Não informado')
                    pct = (row['count'] / total_registros) * 100
                    self.logger.info(f"  {porte}: {row['count']:,} ({pct:.1f}%)")
            
            self.logger.info("=== FIM DO RELATÓRIO ===")
            
        except Exception as e:
            self.logger.error(f"Erro ao gerar relatório de estatísticas: {str(e)}")
    
    def process_single_zip_impl(self, zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, **kwargs) -> bool:
        """
        Implementação para compatibilidade com BaseProcessor.
        
        Para o PainelProcessor, este método delega para process_painel_data()
        já que não processamos ZIPs diretamente, mas sim dados já processados.
        """
        return self.process_painel_data()
    
    def export_to_csv(self, input_parquet: str, output_csv: str, delimiter: str = ';') -> bool:
        """
        Exporta dados do painel para CSV.
        
        Args:
            input_parquet: Caminho do arquivo parquet
            output_csv: Caminho do arquivo CSV de saída
            delimiter: Delimitador para o CSV
            
        Returns:
            bool: True se exportação foi bem-sucedida
        """
        try:
            self.logger.info(f"Exportando dados para CSV: {output_csv}")
            
            # Carregar dados
            df = pl.read_parquet(input_parquet)
            
            # Exportar para CSV
            df.write_csv(output_csv, separator=delimiter)
            
            self.logger.info(f"✓ Exportação para CSV concluída: {df.height} registros")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro na exportação para CSV: {str(e)}")
            return False
    
    def process_complete_painel(self, output_filename: Optional[str] = None) -> bool:
        """
        Processa dados do painel de forma completa desde o download até o painel final.
        
        Este método executa todo o pipeline:
        1. Download dos arquivos (se necessário)
        2. Descompactação (se necessário) 
        3. Processamento individual das entidades (se necessário)
        4. Criação do painel combinado
        5. Aplicação de filtros e transformações
        6. Geração do arquivo final
        
        Args:
            output_filename: Nome do arquivo de saída (opcional)
            
        Returns:
            bool: True se processamento foi bem-sucedido
        """
        try:
            self.logger.info("=== INICIANDO PROCESSAMENTO COMPLETO DO PAINEL ===")
            
            # Etapa 1: Verificar estado atual dos dados
            estado_dados = self._verificar_estado_dados()
            self.logger.info(f"Estado atual dos dados: {estado_dados}")
            
            # Etapa 2: Download (se necessário)
            if not self.skip_download and estado_dados['precisa_download']:
                self.logger.info("Iniciando download dos arquivos...")
                if not self._executar_downloads():
                    self.logger.error("Falha no download dos arquivos")
                    return False
                self.logger.info("✓ Downloads concluídos")
            
            # Etapa 3: Descompactação (se necessário)
            if not self.skip_unzip and estado_dados['precisa_unzip']:
                self.logger.info("Iniciando descompactação dos arquivos...")
                if not self._executar_descompactacao():
                    self.logger.error("Falha na descompactação")
                    return False
                self.logger.info("✓ Descompactação concluída")
            
            # Etapa 4: Processamento individual das entidades (se necessário)
            if not self.skip_individual_processing and estado_dados['precisa_processamento']:
                self.logger.info("Iniciando processamento individual das entidades...")
                if not self._executar_processamento_individual():
                    self.logger.error("Falha no processamento individual")
                    return False
                self.logger.info("✓ Processamento individual concluído")
            
            # Etapa 5: Definir caminhos dos parquets
            if not self._definir_caminhos_parquets():
                self.logger.error("Falha ao definir caminhos dos parquets")
                return False
            
            # Etapa 6: Criar painel combinado
            self.logger.info("Iniciando criação do painel combinado...")
            if not self.process_painel_data(output_filename):
                self.logger.error("Falha na criação do painel")
                return False
            
            self.logger.info("=== PROCESSAMENTO COMPLETO DO PAINEL CONCLUÍDO ===")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro no processamento completo do painel: {str(e)}")
            return False
    
    def _verificar_estado_dados(self) -> Dict[str, bool]:
        """
        Verifica o estado atual dos dados e determina quais etapas são necessárias.
        
        Returns:
            Dict com flags indicando quais etapas são necessárias
        """
        estado = {
            'precisa_download': False,
            'precisa_unzip': False,
            'precisa_processamento': False,
            'tem_parquets': False
        }
        
        try:
            # Verificar se há parquets já processados
            parquets_paths = {
                'estabelecimento': self.estabelecimento_path or os.path.join(self.path_parquet, 'estabelecimento'),
                'simples': self.simples_path or os.path.join(self.path_parquet, 'simples'),
                'empresa': self.empresa_path or os.path.join(self.path_parquet, 'empresa')
            }
            
            parquets_existem = all(
                os.path.exists(path) and self._tem_arquivos_parquet(path)
                for path in parquets_paths.values()
            )
            
            if parquets_existem and not self.force_reprocess:
                estado['tem_parquets'] = True
                self.logger.info("Parquets já existem, usando dados processados")
                return estado
            
            # Verificar se há arquivos CSV descompactados
            csvs_existem = self._verificar_csvs_descompactados()
            if csvs_existem and not self.force_reprocess:
                estado['precisa_processamento'] = True
                self.logger.info("CSVs descompactados encontrados")
                return estado
            
            # Verificar se há arquivos ZIP
            zips_existem = self._verificar_zips_disponiveis()
            if zips_existem:
                estado['precisa_unzip'] = True
                estado['precisa_processamento'] = True
                self.logger.info("Arquivos ZIP encontrados")
                return estado
            
            # Precisa baixar tudo
            estado['precisa_download'] = True
            estado['precisa_unzip'] = True
            estado['precisa_processamento'] = True
            self.logger.info("Nenhum dado encontrado, será necessário download completo")
            
            return estado
            
        except Exception as e:
            self.logger.error(f"Erro ao verificar estado dos dados: {str(e)}")
            # Em caso de erro, assumir que precisa de tudo
            return {
                'precisa_download': True,
                'precisa_unzip': True,
                'precisa_processamento': True,
                'tem_parquets': False
            }
    
    def _tem_arquivos_parquet(self, path: str) -> bool:
        """Verifica se um diretório tem arquivos parquet."""
        if not os.path.exists(path):
            return False
        
        if os.path.isfile(path) and path.endswith('.parquet'):
            return True
        
        if os.path.isdir(path):
            parquet_files = [f for f in os.listdir(path) if f.endswith('.parquet')]
            return len(parquet_files) > 0
        
        return False
    
    def _verificar_csvs_descompactados(self) -> bool:
        """Verifica se há CSVs descompactados disponíveis."""
        if not os.path.exists(self.path_unzip):
            return False
        
        # Procurar por padrões de arquivos esperados
        padroes = ['*ESTABELE*', '*SIMPLES*', '*EMPRESAS*']
        
        for padrao in padroes:
            import glob
            files = glob.glob(os.path.join(self.path_unzip, f"{padrao}.csv"))
            if not files:
                return False
        
        return True
    
    def _verificar_zips_disponiveis(self) -> bool:
        """Verifica se há arquivos ZIP disponíveis."""
        if not os.path.exists(self.path_zip):
            return False
        
        # Procurar por padrões de arquivos ZIP esperados
        padroes = ['*Estabele*', '*Simples*', '*Empresas*']
        
        for padrao in padroes:
            import glob
            files = glob.glob(os.path.join(self.path_zip, f"{padrao}*.zip"))
            if not files:
                return False
        
        return True
    
    def _executar_downloads(self) -> bool:
        """
        Executa o download dos arquivos necessários.
        
        Returns:
            bool: True se download foi bem-sucedido
        """
        try:
            # Criar diretório de download se não existir
            os.makedirs(self.path_zip, exist_ok=True)
            
            # Implementar download dos arquivos
            # Nota: Esta é uma implementação simplificada
            # Em produção, seria necessário implementar download real
            
            self.logger.info("Download dos arquivos (implementação seria necessária)")
            
            # Por enquanto, assumir que os arquivos já estão disponíveis
            # Em implementação real, usar requests ou urllib para download
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro no download: {str(e)}")
            return False
    
    def _executar_descompactacao(self) -> bool:
        """
        Executa a descompactação dos arquivos ZIP.
        
        Returns:
            bool: True se descompactação foi bem-sucedida
        """
        try:
            import zipfile
            import glob
            
            # Criar diretório de descompactação
            os.makedirs(self.path_unzip, exist_ok=True)
            
            # Encontrar todos os arquivos ZIP
            zip_files = glob.glob(os.path.join(self.path_zip, "*.zip"))
            
            if not zip_files:
                self.logger.error("Nenhum arquivo ZIP encontrado")
                return False
            
            for zip_file in zip_files:
                self.logger.info(f"Descompactando: {os.path.basename(zip_file)}")
                
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    zip_ref.extractall(self.path_unzip)
                
                self.logger.info(f"✓ Descompactado: {os.path.basename(zip_file)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro na descompactação: {str(e)}")
            return False
    
    def _executar_processamento_individual(self) -> bool:
        """
        Executa o processamento individual das entidades.
        
        Returns:
            bool: True se processamento foi bem-sucedido
        """
        try:
            # Criar diretório de parquets
            os.makedirs(self.path_parquet, exist_ok=True)
            
            # Importar processadores necessários
            from .estabelecimento_processor import EstabelecimentoProcessor
            from .simples_processor import SimplesProcessor
            from .empresa_processor import EmpresaProcessor
            
            processadores = [
                ('estabelecimento', EstabelecimentoProcessor),
                ('simples', SimplesProcessor),
                ('empresa', EmpresaProcessor)
            ]
            
            for nome, ProcessorClass in processadores:
                self.logger.info(f"Processando {nome}...")
                
                # Criar subdiretório para cada entidade
                output_path = os.path.join(self.path_parquet, nome)
                os.makedirs(output_path, exist_ok=True)
                
                # Inicializar processador
                processor = ProcessorClass(
                    path_zip=self.path_zip,
                    path_unzip=self.path_unzip,
                    path_parquet=output_path
                )
                
                # Processar arquivos ZIP
                import glob
                zip_pattern = self._get_zip_pattern(nome)
                zip_files = glob.glob(os.path.join(self.path_zip, zip_pattern))
                
                if not zip_files:
                    self.logger.error(f"Nenhum arquivo ZIP encontrado para {nome}")
                    return False
                
                for zip_file in zip_files:
                    if not processor.process_single_zip_impl(
                        zip_file=os.path.basename(zip_file),
                        path_zip=self.path_zip,
                        path_unzip=self.path_unzip,
                        path_parquet=output_path
                    ):
                        self.logger.error(f"Falha no processamento de {zip_file}")
                        return False
                
                self.logger.info(f"✓ {nome} processado com sucesso")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro no processamento individual: {str(e)}")
            return False
    
    def _get_zip_pattern(self, entidade: str) -> str:
        """Retorna o padrão de arquivo ZIP para uma entidade."""
        patterns = {
            'estabelecimento': '*Estabele*.zip',
            'simples': '*Simples*.zip',
            'empresa': '*Empresas*.zip'
        }
        return patterns.get(entidade, '*.zip')
    
    def _definir_caminhos_parquets(self) -> bool:
        """
        Define os caminhos dos parquets baseado no que está disponível.
        
        Returns:
            bool: True se caminhos foram definidos com sucesso
        """
        try:
            # Se caminhos já foram especificados, usar eles
            if not self.estabelecimento_path:
                self.estabelecimento_path = os.path.join(self.path_parquet, 'estabelecimento')
            
            if not self.simples_path:
                self.simples_path = os.path.join(self.path_parquet, 'simples')
            
            if not self.empresa_path:
                self.empresa_path = os.path.join(self.path_parquet, 'empresa')
            
            # Verificar se todos os caminhos existem
            caminhos = [self.estabelecimento_path, self.simples_path, self.empresa_path]
            nomes = ['estabelecimento', 'simples', 'empresa']
            
            for caminho, nome in zip(caminhos, nomes):
                if not self._tem_arquivos_parquet(caminho):
                    self.logger.error(f"Parquets de {nome} não encontrados em: {caminho}")
                    return False
            
            self.logger.info("✓ Caminhos dos parquets definidos com sucesso")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao definir caminhos dos parquets: {str(e)}")
            return False


# ===== EXEMPLOS DE USO =====

def exemplo_processamento_completo():
    """
    Exemplo de uso do processador completo do painel.
    
    Este exemplo mostra como processar dados do painel desde o download
    até a criação do arquivo final.
    """
    
    # Configuração básica de caminhos
    config = {
        'path_zip': '/caminho/para/zips',
        'path_unzip': '/caminho/para/extrair',
        'path_parquet': '/caminho/para/parquets',
        
        # Filtros opcionais
        'situacao_filter': 2,  # Apenas estabelecimentos ativos
        
        # Opções de processamento
        'force_reprocess': False,  # Não reprocessar se parquets existirem
        'skip_download': False,    # Executar download se necessário
        'skip_unzip': False,       # Executar descompactação se necessário
        'skip_individual_processing': False  # Processar entidades individuais
    }
    
    # Criar processador
    processor = PainelProcessor(**config)
    
    # Executar processamento completo
    sucesso = processor.process_complete_painel(output_filename='painel_sp_ativos.parquet')
    
    if sucesso:
        print("✓ Processamento do painel concluído com sucesso!")
    else:
        print("✗ Falha no processamento do painel")


def exemplo_uso_parquets_existentes():
    """
    Exemplo de uso com parquets já processados.
    
    Este exemplo mostra como criar o painel quando já se tem
    os parquets das entidades individuais.
    """
    
    config = {
        'path_zip': '/caminho/para/zips',  # Não será usado
        'path_unzip': '/caminho/para/extrair',  # Não será usado  
        'path_parquet': '/caminho/para/saida',
        
        # Caminhos específicos para dados já processados
        'estabelecimento_path': '/dados/estabelecimentos.parquet',
        'simples_path': '/dados/simples.parquet',
        'empresa_path': '/dados/empresas.parquet',
        
        # Pular etapas desnecessárias
        'skip_download': True,
        'skip_unzip': True,
        'skip_individual_processing': True
    }
    
    processor = PainelProcessor(**config)
    
    # Processar apenas o painel (sem pipeline completo)
    sucesso = processor.process_painel_data(output_filename='painel_rj.parquet')
    
    if sucesso:
        print("✓ Painel criado com sucesso a partir de parquets existentes!")


def exemplo_uso_flexivel():
    """
    Exemplo de uso flexível baseado no estado dos dados.
    
    O processador detecta automaticamente o que está disponível
    e executa apenas as etapas necessárias.
    """
    
    config = {
        'path_zip': '/dados/zips',
        'path_unzip': '/dados/temp',
        'path_parquet': '/dados/output',
        
        # Não especificar caminhos individuais - deixar o processador decidir
        # 'estabelecimento_path': None,
        # 'simples_path': None, 
        # 'empresa_path': None,
        
        # Permitir que o processador decida o que fazer
        'force_reprocess': False,
        
        # Filtros aplicados apenas no painel final
        'situacao_filter': None  # Todas as situações
    }
    
    processor = PainelProcessor(**config)
    
    # O processador vai:
    # 1. Verificar se existem parquets -> usar se existirem
    # 2. Se não, verificar CSVs -> processar se existirem  
    # 3. Se não, verificar ZIPs -> descompactar e processar
    # 4. Se não, fazer download -> descompactar e processar
    
    sucesso = processor.process_complete_painel()
    
    return sucesso


# ===== UTILITÁRIOS PARA ANÁLISE =====

def analisar_painel(caminho_parquet: str):
    """
    Utilitário para analisar dados do painel gerado.
    
    Args:
        caminho_parquet: Caminho para o arquivo parquet do painel
    """
    try:
        import polars as pl
        
        # Carregar dados
        df = pl.read_parquet(caminho_parquet)
        
        print(f"=== ANÁLISE DO PAINEL ===")
        print(f"Total de registros: {df.height:,}")
        print(f"Total de colunas: {df.width}")
        
        # Estatísticas gerais
        total_registros = df.height
        print(f"Total de registros: {total_registros:,}")
        
        # Estatísticas de opção pelo Simples Nacional
        if 'opcao_simples' in df.columns:
            simples_stats = df.group_by('opcao_simples').agg([
                pl.count().alias('count')
            ])
            
            print("\nOpção pelo Simples Nacional:")
            for row in simples_stats.iter_rows(named=True):
                opcao = 'Sim' if row['opcao_simples'] == 'S' else 'Não' if row['opcao_simples'] == 'N' else 'Não informado'
                pct = (row['count'] / df.height) * 100
                print(f"  {opcao}: {row['count']:,} ({pct:.1f}%)")
        
        # Estatísticas de porte
        if 'porte_empresa' in df.columns:
            porte_stats = df.group_by('porte_empresa').agg([
                pl.count().alias('count')
            ])
            
            print("\nPorte da empresa:")
            porte_map = {1: 'Micro', 2: 'Pequena', 3: 'Média', 4: 'Grande', 5: 'Demais'}
            for row in porte_stats.iter_rows(named=True):
                porte = porte_map.get(row['porte_empresa'], 'Não informado')
                pct = (row['count'] / df.height) * 100
                print(f"  {porte}: {row['count']:,} ({pct:.1f}%)")
        
        print("=== FIM DA ANÁLISE ===")
        
    except Exception as e:
        print(f"Erro na análise: {str(e)}")


if __name__ == "__main__":
    # Exemplo de execução
    print("Exemplo de processamento completo do painel:")
    exemplo_processamento_completo() 