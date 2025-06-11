"""
Exemplo de uso da entidade e processador Painel.

Este script demonstra como usar o sistema para combinar dados de 
Estabelecimento e Simples Nacional em uma visão consolidada.
"""

import os
import sys
import logging
from datetime import datetime

# Adicionar o diretório src ao path para importações
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from Entity.Painel import Painel
from process.processors.painel_processor import PainelProcessor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def exemplo_entidade_painel():
    """Exemplo de uso da entidade Painel."""
    logger.info("=== EXEMPLO DE USO DA ENTIDADE PAINEL ===")
    
    # Criar instância da entidade Painel
    painel = Painel(
        cnpj_basico="12345678",
        cnpj_completo="12345678000195",
        matriz_filial=1,
        nome_fantasia="EMPRESA EXEMPLO LTDA",
        codigo_situacao=2,
        data_situacao_cadastral=datetime(2020, 1, 15),
        codigo_motivo=0,
        data_inicio_atividades=datetime(2015, 6, 10),
        codigo_cnae=4711301,
        uf="SP",
        codigo_municipio=3550308,
        cep="01310100",
        
        # Dados do Simples Nacional
        opcao_simples="S",
        data_opcao_simples=datetime(2015, 7, 1),
        opcao_mei="N"
    )
    
    # Verificar se é válido
    if painel.is_valid():
        logger.info("✓ Entidade Painel criada e validada com sucesso")
        
        # Demonstrar métodos de conveniência
        logger.info(f"É matriz: {painel.is_matriz()}")
        logger.info(f"Estabelecimento ativo: {painel.is_ativo_estabelecimento()}")
        logger.info(f"Optante do Simples: {painel.is_optante_simples()}")
        logger.info(f"Ativo no Simples: {painel.is_ativo_simples()}")
        logger.info(f"CNPJ formatado: {painel.cnpj_formatado}")
        logger.info(f"CEP formatado: {painel.cep_formatado}")
        logger.info(f"Situação Simples: {painel.situacao_simples}")
        logger.info(f"Situação MEI: {painel.situacao_mei}")
        
        # Resumo do status
        resumo = painel.get_resumo_status()
        logger.info(f"Resumo do status: {resumo}")
        
    else:
        logger.error("✗ Entidade Painel inválida")
        logger.error(f"Erros: {painel.get_validation_errors()}")


def exemplo_processador_painel():
    """Exemplo de uso do processador Painel."""
    logger.info("=== EXEMPLO DE USO DO PROCESSADOR PAINEL ===")
    
    # Caminhos de exemplo (ajustar conforme seu ambiente)
    estabelecimento_path = "data/processed/estabelecimento"
    simples_path = "data/processed/simples"
    output_path = "data/exports/painel"
    
    try:
        # Criar processador
        processor = PainelProcessor(
            path_zip="",  # Não usado para painel
            path_unzip="",  # Não usado para painel
            path_parquet=output_path,
            estabelecimento_path=estabelecimento_path,
            simples_path=simples_path,
            uf_filter="SP",  # Filtrar apenas SP
            include_inactive=False  # Apenas estabelecimentos ativos
        )
        
        logger.info(f"Processador criado: {processor.get_processor_name()}")
        logger.info(f"Entidade associada: {processor.get_entity_class().__name__}")
        logger.info(f"Opções válidas: {processor.get_valid_options()}")
        
        # Processar dados
        success = processor.process_painel_data("painel_sp_ativos.parquet")
        
        if success:
            logger.info("✓ Processamento do Painel concluído com sucesso")
            
            # Exportar para CSV
            parquet_file = os.path.join(output_path, "painel_sp_ativos.parquet")
            csv_file = os.path.join(output_path, "painel_sp_ativos.csv")
            
            if os.path.exists(parquet_file):
                export_success = processor.export_to_csv(parquet_file, csv_file)
                if export_success:
                    logger.info("✓ Exportação para CSV concluída")
                else:
                    logger.error("✗ Falha na exportação para CSV")
        else:
            logger.error("✗ Falha no processamento do Painel")
            
    except ValueError as e:
        logger.error(f"Erro de configuração: {e}")
        logger.info("Certifique-se de que os caminhos para estabelecimento e simples estão corretos")
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")


def exemplo_processamento_por_uf():
    """Exemplo de processamento separado por UF."""
    logger.info("=== EXEMPLO DE PROCESSAMENTO POR UF ===")
    
    ufs_interesse = ["SP", "RJ", "MG", "RS", "PR"]
    
    for uf in ufs_interesse:
        logger.info(f"Processando UF: {uf}")
        
        try:
            processor = PainelProcessor(
                path_zip="",
                path_unzip="",
                path_parquet="data/exports/painel",
                estabelecimento_path="data/processed/estabelecimento",
                simples_path="data/processed/simples",
                uf_filter=uf,
                include_inactive=False
            )
            
            output_file = f"painel_{uf.lower()}_ativos.parquet"
            success = processor.process_painel_data(output_file)
            
            if success:
                logger.info(f"✓ UF {uf} processada com sucesso")
            else:
                logger.error(f"✗ Falha no processamento da UF {uf}")
                
        except Exception as e:
            logger.error(f"Erro no processamento da UF {uf}: {e}")


def exemplo_filtros_avancados():
    """Exemplo de uso com filtros avançados."""
    logger.info("=== EXEMPLO DE FILTROS AVANÇADOS ===")
    
    # Filtros diferentes para diferentes necessidades
    filtros = [
        {
            "nome": "Empresas ativas no Simples - SP",
            "uf_filter": "SP",
            "situacao_filter": "Ativo",
            "include_inactive": False,
            "output": "painel_sp_simples_ativo.parquet"
        },
        {
            "nome": "Todos estabelecimentos - RJ",
            "uf_filter": "RJ",
            "situacao_filter": None,
            "include_inactive": True,
            "output": "painel_rj_todos.parquet"
        },
        {
            "nome": "Não optantes do Simples - MG",
            "uf_filter": "MG",
            "situacao_filter": "Não optante",
            "include_inactive": False,
            "output": "painel_mg_nao_optantes.parquet"
        }
    ]
    
    for filtro in filtros:
        logger.info(f"Processando: {filtro['nome']}")
        
        try:
            processor = PainelProcessor(
                path_zip="",
                path_unzip="",
                path_parquet="data/exports/painel",
                estabelecimento_path="data/processed/estabelecimento",
                simples_path="data/processed/simples",
                **{k: v for k, v in filtro.items() if k not in ['nome', 'output']}
            )
            
            success = processor.process_painel_data(filtro['output'])
            
            if success:
                logger.info(f"✓ {filtro['nome']} processado com sucesso")
            else:
                logger.error(f"✗ Falha no processamento: {filtro['nome']}")
                
        except Exception as e:
            logger.error(f"Erro em {filtro['nome']}: {e}")


def main():
    """Função principal que executa todos os exemplos."""
    logger.info("Iniciando exemplos de uso do sistema Painel")
    
    try:
        # Exemplo 1: Uso da entidade
        exemplo_entidade_painel()
        
        print("\n" + "="*60 + "\n")
        
        # Exemplo 2: Uso do processador (comentado pois precisa de dados reais)
        # exemplo_processador_painel()
        
        # Exemplo 3: Processamento por UF (comentado pois precisa de dados reais)
        # exemplo_processamento_por_uf()
        
        # Exemplo 4: Filtros avançados (comentado pois precisa de dados reais)
        # exemplo_filtros_avancados()
        
        logger.info("Para executar os exemplos de processamento, descomente as linhas")
        logger.info("correspondentes em main() e ajuste os caminhos dos dados.")
        
    except Exception as e:
        logger.error(f"Erro na execução dos exemplos: {e}")


if __name__ == "__main__":
    main() 