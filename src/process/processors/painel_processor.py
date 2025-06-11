"""
Processador de Painel - Combinação de Estabelecimento e Simples Nacional.

Este processador implementa o left join entre dados de estabelecimentos
e dados do Simples Nacional, criando uma visão consolidada para exportação.

Funcionalidades:
- Left join entre estabelecimentos e Simples Nacional por cnpj_basico
- Aplicação de transformações específicas da entidade Painel
- Validação de dados combinados
- Geração de campos calculados (situações, formatações)
- Suporte a filtros por UF e outros critérios
"""

import logging
import os
import polars as pl
from typing import List, Type, Optional, Dict, Any
from datetime import datetime

from ...Entity.Painel import Painel
from ...Entity.base import BaseEntity
from ..base.processor import BaseProcessor

logger = logging.getLogger(__name__)


class PainelProcessor(BaseProcessor):
    """
    Processador específico para dados do Painel (Estabelecimento + Simples).
    
    Características:
    - Faz left join entre estabelecimentos e Simples Nacional
    - Utiliza entidade Painel para validação e transformação
    - Gera campos calculados úteis para exportação
    - Suporte a filtros específicos (UF, situação, etc.)
    - Integração com sistema de fila unificado
    """
    
    def __init__(self, path_zip: str, path_unzip: str, path_parquet: str, **kwargs):
        """
        Inicializa o processador de Painel.
        
        Args:
            path_zip: Diretório com arquivos ZIP (não usado - dados já processados)
            path_unzip: Diretório para extração (não usado)
            path_parquet: Diretório de saída
            **kwargs: Opções específicas
                - estabelecimento_path: Caminho para dados de estabelecimentos
                - simples_path: Caminho para dados do Simples Nacional
                - uf_filter: Filtro por UF (opcional)
                - situacao_filter: Filtro por situação (opcional)
        """
        super().__init__(path_zip, path_unzip, path_parquet, **kwargs)
        
        # Caminhos para dados já processados
        self.estabelecimento_path = kwargs.get('estabelecimento_path')
        self.simples_path = kwargs.get('simples_path')
        
        # Filtros específicos
        self.uf_filter = kwargs.get('uf_filter')
        self.situacao_filter = kwargs.get('situacao_filter')
        
        # Validar caminhos obrigatórios
        if not self.estabelecimento_path:
            raise ValueError("Caminho para dados de estabelecimentos é obrigatório (estabelecimento_path)")
        
        if not self.simples_path:
            raise ValueError("Caminho para dados do Simples Nacional é obrigatório (simples_path)")
    
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
            'uf_filter',
            'situacao_filter',
            'include_inactive'
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
            
            # 2. Aplicar filtros se especificados
            if self.uf_filter:
                df = df.filter(pl.col('uf') == self.uf_filter.upper())
                self.logger.info(f"Filtro UF aplicado: {self.uf_filter}")
            
            if self.situacao_filter:
                df = df.filter(pl.col('codigo_situacao') == self.situacao_filter)
                self.logger.info(f"Filtro situação aplicado: {self.situacao_filter}")
            
            # 3. Remover estabelecimentos inativos se solicitado
            if not self.options.get('include_inactive', True):
                df = df.filter(pl.col('codigo_situacao') == 2)  # Apenas ativos
                self.logger.info("Estabelecimentos inativos removidos")
            
            self.logger.info(f"Transformações específicas aplicadas. Registros finais: {df.height}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao aplicar transformações específicas: {str(e)}")
            raise
    
    def process_painel_data(self, output_filename: Optional[str] = None) -> bool:
        """
        Processa dados do painel fazendo left join entre estabelecimentos e Simples.
        
        Args:
            output_filename: Nome do arquivo de saída (opcional)
            
        Returns:
            bool: True se processamento foi bem-sucedido
        """
        try:
            self.logger.info("Iniciando processamento de dados do Painel")
            
            # 1. Carregar dados de estabelecimentos
            self.logger.info(f"Carregando dados de estabelecimentos: {self.estabelecimento_path}")
            
            if not self.estabelecimento_path:
                raise ValueError("Caminho de estabelecimentos não configurado")
            
            if os.path.isdir(self.estabelecimento_path):
                # Carregar múltiplos arquivos parquet
                estabelecimentos_df = pl.scan_parquet(f"{self.estabelecimento_path}/*.parquet").collect()
            else:
                # Carregar arquivo único
                estabelecimentos_df = pl.read_parquet(self.estabelecimento_path)
            
            self.logger.info(f"Estabelecimentos carregados: {estabelecimentos_df.height} registros")
            
            # 2. Carregar dados do Simples Nacional
            self.logger.info(f"Carregando dados do Simples Nacional: {self.simples_path}")
            
            if not self.simples_path:
                raise ValueError("Caminho de dados do Simples não configurado")
            
            if os.path.isdir(self.simples_path):
                # Carregar múltiplos arquivos parquet
                simples_df = pl.scan_parquet(f"{self.simples_path}/*.parquet").collect()
            else:
                # Carregar arquivo único
                simples_df = pl.read_parquet(self.simples_path)
            
            self.logger.info(f"Dados do Simples carregados: {simples_df.height} registros")
            
            # 3. Garantir que CNPJ básico está padronizado em ambos (como bigint)
            estabelecimentos_df = estabelecimentos_df.with_columns([
                pl.col('cnpj_basico').cast(pl.Int64).alias('cnpj_basico')
            ])
            
            simples_df = simples_df.with_columns([
                pl.col('cnpj_basico').cast(pl.Int64).alias('cnpj_basico')
            ])
            
            # 4. Fazer left join
            self.logger.info("Executando left join entre estabelecimentos e Simples Nacional")
            
            painel_df = estabelecimentos_df.join(
                simples_df,
                on='cnpj_basico',
                how='left'
            )
            
            self.logger.info(f"Join executado. Registros resultantes: {painel_df.height}")
            
            # 5. Aplicar transformações específicas
            painel_df = self.apply_specific_transformations(painel_df)
            
            # 6. Salvar resultado
            if not output_filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"painel_dados_{timestamp}.parquet"
            
            output_path = os.path.join(self.path_parquet, output_filename)
            
            # Criar diretório se não existir
            os.makedirs(self.path_parquet, exist_ok=True)
            
            # Salvar
            painel_df.write_parquet(output_path, compression='snappy')
            
            self.logger.info(f"✓ Dados do Painel salvos: {output_path}")
            self.logger.info(f"✓ Total de registros: {painel_df.height}")
            
            # 7. Relatório de estatísticas
            self._generate_statistics_report(painel_df)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro no processamento do Painel: {str(e)}")
            return False
    
    def _generate_statistics_report(self, df: pl.DataFrame):
        """Gera relatório de estatísticas dos dados processados."""
        try:
            self.logger.info("=== RELATÓRIO DE ESTATÍSTICAS DO PAINEL ===")
            
            # Estatísticas gerais
            total_registros = df.height
            self.logger.info(f"Total de registros: {total_registros:,}")
            
            # Estatísticas por UF
            if 'uf' in df.columns:
                uf_stats = df.group_by('uf').agg([
                    pl.count().alias('count')
                ]).sort('count', descending=True)
                
                self.logger.info("Top 10 UFs por número de estabelecimentos:")
                for row in uf_stats.head(10).iter_rows(named=True):
                    self.logger.info(f"  {row['uf']}: {row['count']:,}")
            
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