"""
Processador de estabelecimentos refatorado.

Implementação específica para processamento de dados de estabelecimentos,
utilizando a infraestrutura unificada da Fase 2 e integrando
com a entidade Estabelecimento da Fase 1.

Funcionalidades específicas:
- Criação de CNPJ completo a partir das partes
- Filtro por UF (uf_subset) para subset por estado
- Processamento otimizado para arquivos grandes
"""

import logging
import os
import zipfile
import polars as pl
import tempfile
from typing import List, Type, Optional

from ...Entity.Estabelecimento import Estabelecimento
from ...Entity.base import BaseEntity
from ..base.processor import BaseProcessor

logger = logging.getLogger(__name__)


class EstabelecimentoProcessor(BaseProcessor):
    """
    Processador específico para dados de estabelecimentos.
    
    Características:
    - Utiliza entidade Estabelecimento para validação e transformação
    - Criação automática de CNPJ completo
    - Funcionalidade uf_subset para filtrar por estado
    - Integração com sistema de fila unificado
    - Otimizado para arquivos grandes (>2GB)
    - Remove toda duplicação de código
    """
    
    def get_processor_name(self) -> str:
        """Retorna o nome do processador."""
        return "ESTABELECIMENTO"
    
    def get_entity_class(self) -> Type[BaseEntity]:
        """Retorna a classe de entidade associada."""
        return Estabelecimento
    
    def get_valid_options(self) -> List[str]:
        """Retorna opções válidas para este processador."""
        return ['uf_subset']  # Funcionalidade específica de estabelecimentos
    
    def apply_specific_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica transformações específicas de estabelecimentos.
        
        Args:
            df: DataFrame a ser transformado
            
        Returns:
            DataFrame transformado
        """
        try:
            # Transformações específicas para estabelecimentos
            
            # 1. Criar CNPJ completo (14 dígitos) a partir das partes
            if 'cnpj_basico' in df.columns and 'cnpj_ordem' in df.columns and 'cnpj_dv' in df.columns:
                try:
                    # Criar colunas auxiliares temporárias para formar o CNPJ completo
                    # cnpj_basico é inteiro, precisa de padding para 8 dígitos
                    df = df.with_columns([
                        pl.col('cnpj_basico').cast(pl.Utf8).str.pad_start(8, '0').alias('_cnpj_basico_temp'),
                        pl.col('cnpj_ordem').cast(pl.Utf8).str.replace_all(r'[^\d]', '').str.pad_start(4, '0').alias('_cnpj_ordem_temp'),
                        pl.col('cnpj_dv').cast(pl.Utf8).str.replace_all(r'[^\d]', '').str.pad_start(2, '0').alias('_cnpj_dv_temp')
                    ])
                    
                    # Criar CNPJ completo
                    df = df.with_columns([
                        (pl.col('_cnpj_basico_temp') + pl.col('_cnpj_ordem_temp') + pl.col('_cnpj_dv_temp')).alias('cnpj_completo')
                    ])
                    
                    # Remover colunas auxiliares temporárias e as partes originais
                    df = df.drop(['_cnpj_basico_temp', '_cnpj_ordem_temp', '_cnpj_dv_temp', 'cnpj_ordem', 'cnpj_dv'])
                    
                    self.logger.debug("CNPJ completo criado com sucesso")
                except Exception as e:
                    self.logger.warning(f"Erro ao criar CNPJ completo: {str(e)}")
            else:
                self.logger.warning("Colunas necessárias para CNPJ completo não encontradas")
            
            # 2. Limpar e validar CEP
            if 'cep' in df.columns:
                df = df.with_columns([
                    pl.col('cep')
                    .str.replace_all(r'[^\d]', '')  # Remove não-dígitos
                    .str.pad_start(8, '0')          # Garante 8 dígitos
                    .alias('cep')
                ])
                
                # Remover CEPs inválidos
                df = df.with_columns([
                    pl.when(pl.col('cep').is_in(['00000000', '99999999']))
                    .then(None)
                    .otherwise(pl.col('cep'))
                    .alias('cep')
                ])
            
            # 3. Normalizar UF
            if 'uf' in df.columns:
                df = df.with_columns([
                    pl.col('uf')
                    .str.strip_chars()
                    .str.to_uppercase()
                    .alias('uf')
                ])
                
                # Validar UFs brasileiras
                ufs_validas = [
                    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
                    'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
                    'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
                ]
                
                df = df.with_columns([
                    pl.when(pl.col('uf').is_in(ufs_validas))
                    .then(pl.col('uf'))
                    .otherwise(None)
                    .alias('uf')
                ])
            
            # 4. Normalizar nome fantasia
            if 'nome_fantasia' in df.columns:
                df = df.with_columns([
                    pl.col('nome_fantasia')
                    .str.strip_chars()
                    .str.to_uppercase()
                    .alias('nome_fantasia')
                ])
                
                # Remover nomes fantasia que são apenas números
                df = df.with_columns([
                    pl.when(
                        pl.col('nome_fantasia').str.contains(r'^[\d\s]*$') |
                        (pl.col('nome_fantasia').str.len_chars() < 3)
                    )
                    .then(None)
                    .otherwise(pl.col('nome_fantasia'))
                    .alias('nome_fantasia')
                ])
            
            # 5. Normalizar outras strings (removido 'pais')
            string_columns = ['nome_cidade_exterior']
            for col in string_columns:
                if col in df.columns:
                    df = df.with_columns([
                        pl.col(col)
                        .str.strip_chars()
                        .str.to_uppercase()
                        .alias(col)
                    ])
            
            # 6. Validar códigos situação e motivo (nomes corretos)
            if 'codigo_situacao' in df.columns:
                df = df.with_columns([
                    pl.when(
                        (pl.col('codigo_situacao') < 1) |
                        (pl.col('codigo_situacao') > 99)
                    )
                    .then(None)
                    .otherwise(pl.col('codigo_situacao'))
                    .alias('codigo_situacao')
                ])
            
            # 7. Validar matriz/filial
            if 'matriz_filial' in df.columns:
                df = df.with_columns([
                    pl.when(pl.col('matriz_filial').is_in([1, 2]))
                    .then(pl.col('matriz_filial'))
                    .otherwise(None)
                    .alias('matriz_filial')
                ])
            
            # 8. Adicionar colunas calculadas
            df = self._add_calculated_columns(df)
            
            self.logger.debug(f"Transformações específicas de estabelecimentos aplicadas. Linhas: {df.height}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao aplicar transformações específicas: {str(e)}")
            return df
    
    def _add_calculated_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Adiciona colunas calculadas úteis.
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame com colunas adicionais
        """
        try:
            # Tipo de situação cadastral baseado em regras específicas
            if 'codigo_situacao' in df.columns and 'codigo_motivo' in df.columns:
                try:
                    df = df.with_columns([
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
                    
                    self.logger.debug("Campo tipo_situacao_cadastral criado com sucesso")
                except Exception as e:
                    self.logger.warning(f"Erro ao criar tipo_situacao_cadastral: {str(e)}")
            else:
                self.logger.warning("Campos codigo_situacao ou codigo_motivo não encontrados para criar tipo_situacao_cadastral")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro geral em _add_calculated_columns: {e}")
            return df
    
    def create_uf_subset(self, df: pl.DataFrame, output_path: str, zip_prefix: str, uf: str) -> bool:
        """
        Cria subset de estabelecimentos filtrado por UF.
        
        Args:
            df: DataFrame principal
            output_path: Caminho de saída
            zip_prefix: Prefixo do arquivo ZIP
            uf: UF a filtrar (ex: 'SP', 'RJ')
            
        Returns:
            bool: True se criou com sucesso
        """
        try:
            uf = uf.upper().strip()
            
            # Filtrar apenas estabelecimentos da UF especificada
            if 'uf' not in df.columns:
                self.logger.warning(f"Coluna 'uf' não encontrada no DataFrame para filtro {uf}")
                return False
            
            df_uf = df.filter(pl.col("uf") == uf)
            
            if df_uf.height == 0:
                self.logger.warning(f"Nenhum estabelecimento encontrado para UF {uf} em {zip_prefix}")
                return True
            
            # Criar subpasta para subsets UF dentro da pasta da entidade
            entity_folder = self.get_processor_name().lower()
            base_output_path = os.path.join(output_path, entity_folder)
            uf_output_path = os.path.join(base_output_path, f"uf_{uf.lower()}")
            os.makedirs(uf_output_path, exist_ok=True)
            
            self.logger.info(f"Criando subset UF {uf}: {df_uf.height} estabelecimentos")
            
            # Salvar subset usando padrão da entidade
            subset_filename = f"{zip_prefix}_{uf.lower()}.parquet"
            subset_path = os.path.join(uf_output_path, subset_filename)
            
            df_uf.write_parquet(subset_path, compression='snappy')
            
            self.logger.info(f"Subset UF {uf} salvo: {entity_folder}/uf_{uf.lower()}/{subset_filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao criar subset UF {uf}: {e}")
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
        Implementação específica do processamento de um ZIP de estabelecimentos.
        
        Args:
            zip_file: Nome do arquivo ZIP
            path_zip: Diretório com arquivos ZIP
            path_unzip: Diretório para extração
            path_parquet: Diretório de saída
            **kwargs: Opções adicionais (incluindo uf_subset)
            
        Returns:
            bool: True se sucesso, False caso contrário
        """
        zip_path = os.path.join(path_zip, zip_file)
        zip_prefix = os.path.splitext(zip_file)[0]
        uf_subset = kwargs.get('uf_subset', None)
        
        try:
            self.logger.info(f"Iniciando processamento de {zip_file} (uf_subset={uf_subset})")
            
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
                
                # 2. Processar arquivos extraídos em chunks (estabelecimentos são grandes)
                chunk_counter = 0
                uf_dataframes = []  # Para acumular dados da UF
                
                for file_name in os.listdir(temp_extract_dir):
                    file_path = os.path.join(temp_extract_dir, file_name)
                    
                    if os.path.isfile(file_path):
                        self.logger.debug(f"Processando arquivo: {file_name}")
                        
                        # Processar arquivo em chunks para estabelecimentos (arquivos grandes)
                        file_chunks = self.process_in_chunks(
                            file_path, 
                            path_parquet, 
                            zip_prefix,
                            chunk_size=500_000,  # Chunks menores para estabelecimentos
                            uf_subset=uf_subset
                        )
                        
                        if file_chunks > 0:
                            chunk_counter += file_chunks
                            self.logger.info(f"Arquivo {file_name} processado: {file_chunks} chunks")
                        else:
                            self.logger.warning(f"Arquivo {file_name} não gerou chunks")
                
                # 3. Verificar se foi gerado algum chunk
                if chunk_counter == 0:
                    self.logger.warning(f"Nenhum chunk válido gerado para {zip_file}")
                    return False
                
                self.logger.info(f"✓ Processamento de {zip_file} concluído: {chunk_counter} chunks")
                return True
                    
        except Exception as e:
            self.logger.error(f"Erro no processamento de {zip_file}: {str(e)}")
            return False
    
    def _extract_zip(self, zip_path: str, extract_dir: str) -> bool:
        """
        Extrai arquivo ZIP com otimização para arquivos grandes.
        
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
        uf_subset: Optional[str] = None
    ) -> int:
        """
        Processa arquivo grande em chunks com filtro por UF.
        
        Args:
            data_file_path: Caminho do arquivo de dados
            output_dir: Diretório de saída
            zip_prefix: Prefixo para nomes dos arquivos
            chunk_size: Tamanho do chunk
            uf_subset: UF para filtrar (opcional)
            
        Returns:
            int: Número de chunks processados
        """
        try:
            self.logger.info(f"Processando arquivo em chunks: {os.path.basename(data_file_path)}")
            
            chunk_counter = 0
            uf_chunks = []  # Para acumular chunks da UF
            
            # Criar subpasta por tipo de entidade (padrão do sistema)
            entity_folder = self.get_processor_name().lower()
            organized_output_path = os.path.join(output_dir, entity_folder)
            os.makedirs(organized_output_path, exist_ok=True)
            
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
                
                # Aplicar filtro de UF se especificado
                if uf_subset and 'uf' in chunk_df.columns:
                    chunk_df = chunk_df.filter(pl.col('uf') == uf_subset.upper())
                    
                    if chunk_df.is_empty():
                        self.logger.debug(f"Chunk {i+1} vazio após filtro UF {uf_subset}")
                        continue
                
                # Salvar chunk principal usando padrão igual aos outros processadores
                chunk_filename = f"{zip_prefix}_part_{i+1:03d}.parquet"
                chunk_path = os.path.join(organized_output_path, chunk_filename)
                
                chunk_df.write_parquet(chunk_path, compression='snappy')
                chunk_counter += 1
                
                # Se filtrou por UF, acumular para subset final
                if uf_subset and chunk_df.height > 0:
                    uf_chunks.append(chunk_df)
                
                self.logger.debug(f"Chunk {i+1}/{num_chunks} salvo: {entity_folder}/{chunk_filename} ({chunk_df.height} linhas)")
            
            # Criar subset por UF se solicitado e há dados
            if uf_subset and uf_chunks:
                combined_uf = pl.concat(uf_chunks, how="vertical_relaxed")
                self.create_uf_subset(combined_uf, output_dir, zip_prefix, uf_subset)
            
            self.logger.info(f"Processamento em chunks concluído: {chunk_counter} chunks em {entity_folder}/")
            return chunk_counter
            
        except Exception as e:
            self.logger.error(f"Erro no processamento em chunks: {str(e)}")
            return 0
    
    def get_processing_summary(self) -> dict:
        """
        Retorna resumo do processamento específico para estabelecimentos.
        
        Returns:
            Dict com informações de resumo
        """
        base_summary = self.get_status()
        
        # Adicionar informações específicas de estabelecimentos
        estabelecimento_summary = {
            **base_summary,
            'entity_type': 'Estabelecimento',
            'supports_chunking': True,
            'default_chunk_size': 500_000,  # Menor que outros por serem arquivos grandes
            'specific_transformations': [
                'Criação de CNPJ completo',
                'Limpeza e validação de CEP',
                'Normalização de UF',
                'Limpeza de nome fantasia',
                'Validação de códigos situação e motivo',
                'Normalização de strings',
                'Remoção de campos desnecessários (pais, cnpj_ordem, cnpj_dv)',
                'Criação de tipo_situacao_cadastral com regras específicas'
            ],
            'output_format': 'Parquet particionado',
            'calculated_columns': [
                'cnpj_completo',
                'tipo_situacao_cadastral'
            ],
            'special_features': [
                'Subset por UF (uf_subset)',
                'Otimização para arquivos grandes',
                'Processamento em chunks inteligente',
                'Validação completa de CNPJ'
            ]
        }
        
        return estabelecimento_summary 