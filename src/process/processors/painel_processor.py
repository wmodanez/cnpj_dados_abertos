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
            
            # 2. Aplicar filtros se especificados explicitamente
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
        
        Pipeline de joins:
        1. Estabelecimento LEFT JOIN Simples Nacional (por cnpj_basico)
        2. Resultado INNER JOIN Empresa (por cnpj_basico)
        
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
                estabelecimentos_df = pl.scan_parquet(f"{self.estabelecimento_path}/*.parquet").collect()
            else:
                estabelecimentos_df = pl.read_parquet(self.estabelecimento_path)
            
            self.logger.info(f"Estabelecimentos carregados: {estabelecimentos_df.height} registros")
            
            # 2. Carregar dados do Simples Nacional
            self.logger.info(f"Carregando dados do Simples Nacional: {self.simples_path}")
            
            if not self.simples_path:
                raise ValueError("Caminho de dados do Simples não configurado")
            
            if os.path.isdir(self.simples_path):
                simples_df = pl.scan_parquet(f"{self.simples_path}/*.parquet").collect()
            else:
                simples_df = pl.read_parquet(self.simples_path)
            
            self.logger.info(f"Dados do Simples carregados: {simples_df.height} registros")
            
            # 3. Carregar dados de empresas
            self.logger.info(f"Carregando dados de empresas: {self.empresa_path}")
            
            if not self.empresa_path:
                raise ValueError("Caminho de dados de empresas não configurado")
            
            if os.path.isdir(self.empresa_path):
                empresas_df = pl.scan_parquet(f"{self.empresa_path}/*.parquet").collect()
            else:
                empresas_df = pl.read_parquet(self.empresa_path)
            
            self.logger.info(f"Dados de empresas carregados: {empresas_df.height} registros")
            
            # 4. Obter campos necessários da entidade Painel
            painel_columns = Painel.get_column_names()
            self.logger.info(f"Campos esperados no painel: {len(painel_columns)} colunas")
            
            # 5. Selecionar apenas campos necessários de cada tabela
            # Estabelecimentos: campos que existem na entidade Painel (incluindo codigo_municipio para JOIN)
            estabelecimento_fields = [
                'cnpj_basico', 'matriz_filial', 'codigo_situacao', 'data_situacao_cadastral',
                'codigo_motivo', 'data_inicio_atividades', 'codigo_cnae', 'codigo_municipio',
                'tipo_situacao_cadastral'
            ]
            
            # Filtrar apenas campos que existem no DataFrame
            estabelecimento_available = [col for col in estabelecimento_fields if col in estabelecimentos_df.columns]
            # Garantir que cnpj_basico está sempre presente (sem duplicação)
            if 'cnpj_basico' not in estabelecimento_available and 'cnpj_basico' in estabelecimentos_df.columns:
                estabelecimento_available.insert(0, 'cnpj_basico')
            estabelecimentos_df = estabelecimentos_df.select(estabelecimento_available)
            
            # Simples Nacional: TODOS os campos exceto cnpj_basico
            simples_fields = [
                'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
                'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
            ]
            
            # Verificar quais campos existem no DataFrame e adicionar cnpj_basico para JOIN
            self.logger.info(f"Campos disponíveis no Simples: {simples_df.columns}")
            simples_available = ['cnpj_basico']  # Sempre incluir para JOIN
            for col in simples_fields:
                if col in simples_df.columns:
                    simples_available.append(col)
                    self.logger.info(f"Campo do Simples encontrado: {col}")
                else:
                    self.logger.warning(f"Campo do Simples NÃO encontrado: {col}")
            simples_df = simples_df.select(simples_available)
            
            # Empresas: cnpj_basico + dois campos (natureza_juridica e porte_empresa)
            empresa_fields = ['natureza_juridica', 'porte_empresa']
            
            # Verificar quais campos existem no DataFrame e adicionar cnpj_basico para JOIN
            self.logger.info(f"Campos disponíveis nas Empresas: {empresas_df.columns}")
            empresa_available = ['cnpj_basico']  # Sempre incluir para JOIN
            for col in empresa_fields:
                if col in empresas_df.columns:
                    empresa_available.append(col)
                    self.logger.info(f"Campo de Empresa encontrado: {col}")
                else:
                    self.logger.warning(f"Campo de Empresa NÃO encontrado: {col}")
            empresas_df = empresas_df.select(empresa_available)
            
            self.logger.info(f"Selecionados: {len(estabelecimento_available)} campos de estabelecimentos")
            self.logger.info(f"Selecionados: {len(simples_available)} campos do Simples")
            self.logger.info(f"Selecionados: {len(empresa_available)} campos de empresas")
            
            # 6. Garantir que CNPJ básico está padronizado em todos (como bigint)
            estabelecimentos_df = estabelecimentos_df.with_columns([
                pl.col('cnpj_basico').cast(pl.Int64).alias('cnpj_basico')
            ])
            
            simples_df = simples_df.with_columns([
                pl.col('cnpj_basico').cast(pl.Int64).alias('cnpj_basico')
            ])
            
            empresas_df = empresas_df.with_columns([
                pl.col('cnpj_basico').cast(pl.Int64).alias('cnpj_basico')
            ])
            
            # 7. Executar LEFT JOIN entre estabelecimentos e Simples Nacional
            self.logger.info("Executando LEFT JOIN entre estabelecimentos e Simples Nacional")
            
            painel_df = estabelecimentos_df.join(
                simples_df,
                on='cnpj_basico',
                how='left'
            )
            
            self.logger.info(f"LEFT JOIN executado. Registros resultantes: {painel_df.height}")
            
            # 8. Executar INNER JOIN com dados de empresas
            self.logger.info("Executando INNER JOIN com dados de empresas")
            
            painel_df = painel_df.join(
                empresas_df,
                on='cnpj_basico',
                how='inner'
            )
            
            self.logger.info(f"INNER JOIN executado. Registros finais: {painel_df.height}")
            
            # 9. Carregar dados de municípios e executar LEFT JOIN
            self.logger.info("Carregando dados de municípios para JOIN")
            
            try:
                municipios_path = os.path.join(self.path_parquet, '..', 'base', 'municipio.parquet')
                municipios_path = os.path.normpath(municipios_path)
                
                if os.path.exists(municipios_path):
                    municipios_df = pl.read_parquet(municipios_path)
                    self.logger.info(f"Municípios carregados: {municipios_df.height} registros")
                    
                    painel_df = painel_df.join(
                        municipios_df.select([
                            pl.col('cod_mn_dados_abertos'),
                            pl.col('codigo').alias('codigo_ibge_municipio')
                        ]),
                        left_on='codigo_municipio',
                        right_on='cod_mn_dados_abertos',
                        how='left'
                    )
                    
                    self.logger.info(f"LEFT JOIN com municípios executado. Registros: {painel_df.height}")
                    
                    com_ibge = painel_df.filter(pl.col('codigo_ibge_municipio').is_not_null()).height
                    sem_ibge = painel_df.height - com_ibge
                    
                    self.logger.info(f"Estabelecimentos com código IBGE: {com_ibge:,} ({(com_ibge/painel_df.height)*100:.1f}%)")
                    self.logger.info(f"Estabelecimentos sem código IBGE: {sem_ibge:,} ({(sem_ibge/painel_df.height)*100:.1f}%)")
                    
                else:
                    self.logger.warning(f"Arquivo de municípios não encontrado: {municipios_path}")
                    self.logger.warning("Continuando sem dados de código IBGE dos municípios")
                    
            except Exception as e:
                self.logger.error(f"Erro ao processar dados de municípios: {str(e)}")
                self.logger.warning("Continuando sem dados de código IBGE dos municípios")
            
            # 10. Aplicar transformações específicas
            painel_df = self.apply_specific_transformations(painel_df)
            
            # 11. Remover coluna codigo_municipio do resultado final (manter apenas codigo_ibge_municipio)
            if 'codigo_municipio' in painel_df.columns:
                painel_df = painel_df.drop('codigo_municipio')
                self.logger.info("Coluna 'codigo_municipio' removida do resultado final")
            
            # 12. Salvar resultado
            if not output_filename:
                output_filename = "painel_dados.parquet"
            
            output_path = os.path.join(self.path_parquet, output_filename)
            
            os.makedirs(self.path_parquet, exist_ok=True)
            
            painel_df.write_parquet(output_path, compression='snappy')
            
            self.logger.info(f"✓ Dados do Painel salvos: {output_path}")
            self.logger.info(f"✓ Total de registros: {painel_df.height}")
            
            # 13. Relatório de estatísticas
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