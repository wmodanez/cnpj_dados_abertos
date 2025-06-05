"""
Processamento em lote de validações para DataFrames grandes.

Este módulo contém funções otimizadas para validar grandes volumes
de dados de forma eficiente.
"""

from typing import Dict, Any, List, Optional
import polars as pl
import logging
from .validator import EntityValidator

logger = logging.getLogger(__name__)


def validate_dataframe_batch(df: pl.DataFrame, entity_type: str, 
                           chunk_size: int = 10000,
                           strict_mode: bool = False,
                           max_workers: int = 1) -> Dict[str, Any]:
    """
    Valida DataFrame grande em lotes.
    
    Args:
        df: DataFrame para validar
        entity_type: Tipo da entidade
        chunk_size: Tamanho do lote
        strict_mode: Modo estrito de validação
        max_workers: Número de workers paralelos (futuro)
        
    Returns:
        Dict[str, Any]: Resultado consolidado da validação
    """
    logger.info(f"Iniciando validação em lote de {df.height:,} linhas de {entity_type}")
    
    validator = EntityValidator(entity_type, strict_mode)
    
    total_results = {
        'entity_type': entity_type,
        'total_rows': df.height,
        'validated_rows': 0,
        'valid_rows': 0,
        'invalid_rows': 0,
        'corrected_rows': 0,
        'success_rate': 0.0,
        'error_summary': {},
        'warnings': [],
        'processing_time': 0.0,
        'chunks_processed': 0
    }
    
    import time
    start_time = time.time()
    
    # Processar em lotes
    chunk_count = 0
    for i in range(0, df.height, chunk_size):
        chunk = df.slice(i, chunk_size)
        chunk_results = validator.validate_dataframe(chunk)
        
        # Consolidar resultados
        total_results['validated_rows'] += chunk_results['validated_rows']
        total_results['valid_rows'] += chunk_results['valid_rows']
        total_results['invalid_rows'] += chunk_results['invalid_rows']
        total_results['corrected_rows'] += chunk_results.get('corrected_rows', 0)
        
        # Consolidar erros
        for error_type, count in chunk_results['error_summary'].items():
            if error_type not in total_results['error_summary']:
                total_results['error_summary'][error_type] = 0
            total_results['error_summary'][error_type] += count
        
        chunk_count += 1
        total_results['chunks_processed'] = chunk_count
        
        # Log de progresso
        progress = (i + chunk_size) / df.height * 100
        logger.info(f"Processado lote {chunk_count}: {chunk.height:,} linhas ({progress:.1f}%)")
    
    # Calcular estatísticas finais
    if total_results['validated_rows'] > 0:
        total_results['success_rate'] = (
            total_results['valid_rows'] / total_results['validated_rows'] * 100
        )
    
    total_results['processing_time'] = time.time() - start_time
    
    logger.info(
        f"Validação em lote concluída: {total_results['success_rate']:.1f}% de sucesso "
        f"em {total_results['processing_time']:.2f}s"
    )
    
    return total_results


def clean_dataframe_batch(df: pl.DataFrame, entity_type: str,
                         chunk_size: int = 10000,
                         remove_invalid: bool = True,
                         attempt_correction: bool = True) -> pl.DataFrame:
    """
    Limpa DataFrame grande em lotes.
    
    Args:
        df: DataFrame para limpar
        entity_type: Tipo da entidade
        chunk_size: Tamanho do lote
        remove_invalid: Se deve remover linhas inválidas
        attempt_correction: Se deve tentar corrigir dados
        
    Returns:
        pl.DataFrame: DataFrame limpo
    """
    logger.info(f"Iniciando limpeza em lote de {df.height:,} linhas de {entity_type}")
    
    validator = EntityValidator(entity_type, strict_mode=False)
    
    cleaned_chunks = []
    total_original = df.height
    total_cleaned = 0
    total_corrected = 0
    
    import time
    start_time = time.time()
    
    # Processar em lotes
    chunk_count = 0
    for i in range(0, df.height, chunk_size):
        chunk = df.slice(i, chunk_size)
        
        cleaned_chunk, cleaning_report = validator.clean_dataframe(
            chunk, 
            remove_invalid=remove_invalid,
            attempt_correction=attempt_correction
        )
        
        if cleaned_chunk.height > 0:
            cleaned_chunks.append(cleaned_chunk)
        
        total_cleaned += cleaning_report['cleaned_rows']
        total_corrected += cleaning_report['corrected_rows']
        
        chunk_count += 1
        
        # Log de progresso
        progress = (i + chunk_size) / df.height * 100
        logger.info(
            f"Processado lote {chunk_count}: {cleaning_report['cleaned_rows']:,} linhas válidas "
            f"({progress:.1f}%)"
        )
    
    # Concatenar chunks limpos
    if cleaned_chunks:
        final_df = pl.concat(cleaned_chunks)
    else:
        # Retornar DataFrame vazio com mesmas colunas
        final_df = df.head(0)
    
    processing_time = time.time() - start_time
    success_rate = total_cleaned / total_original * 100 if total_original > 0 else 0
    
    logger.info(
        f"Limpeza em lote concluída: {total_cleaned:,} linhas válidas "
        f"({success_rate:.1f}%), {total_corrected:,} corrigidas "
        f"em {processing_time:.2f}s"
    )
    
    return final_df


def validate_multiple_entities(dataframes: Dict[str, pl.DataFrame],
                              chunk_size: int = 10000,
                              strict_mode: bool = False) -> Dict[str, Any]:
    """
    Valida múltiplos DataFrames de diferentes entidades.
    
    Args:
        dataframes: Dicionário {entity_type: dataframe}
        chunk_size: Tamanho do lote
        strict_mode: Modo estrito de validação
        
    Returns:
        Dict[str, Any]: Resultados consolidados por entidade
    """
    logger.info(f"Iniciando validação de {len(dataframes)} tipos de entidade")
    
    results = {}
    
    for entity_type, df in dataframes.items():
        logger.info(f"Validando {entity_type}: {df.height:,} linhas")
        
        try:
            entity_results = validate_dataframe_batch(
                df, entity_type, chunk_size, strict_mode
            )
            results[entity_type] = entity_results
            
        except Exception as e:
            logger.error(f"Erro ao validar {entity_type}: {str(e)}")
            results[entity_type] = {
                'entity_type': entity_type,
                'error': str(e),
                'success': False
            }
    
    return results


def create_validation_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Cria resumo consolidado de múltiplas validações.
    
    Args:
        results: Lista de resultados de validação
        
    Returns:
        Dict[str, Any]: Resumo consolidado
    """
    summary = {
        'total_validations': len(results),
        'entities_validated': set(),
        'total_rows': 0,
        'total_valid': 0,
        'total_invalid': 0,
        'total_corrected': 0,
        'overall_success_rate': 0.0,
        'by_entity': {},
        'most_common_errors': {},
        'processing_summary': {
            'total_time': 0.0,
            'total_chunks': 0,
            'avg_time_per_chunk': 0.0
        }
    }
    
    for result in results:
        if 'error' in result:
            continue
            
        entity_type = result['entity_type']
        summary['entities_validated'].add(entity_type)
        summary['total_rows'] += result['total_rows']
        summary['total_valid'] += result['valid_rows']
        summary['total_invalid'] += result['invalid_rows']
        summary['total_corrected'] += result.get('corrected_rows', 0)
        
        # Resumo por entidade
        if entity_type not in summary['by_entity']:
            summary['by_entity'][entity_type] = {
                'total_rows': 0,
                'valid_rows': 0,
                'invalid_rows': 0,
                'corrected_rows': 0,
                'success_rate': 0.0
            }
        
        entity_summary = summary['by_entity'][entity_type]
        entity_summary['total_rows'] += result['total_rows']
        entity_summary['valid_rows'] += result['valid_rows']
        entity_summary['invalid_rows'] += result['invalid_rows']
        entity_summary['corrected_rows'] += result.get('corrected_rows', 0)
        
        if entity_summary['total_rows'] > 0:
            entity_summary['success_rate'] = (
                entity_summary['valid_rows'] / entity_summary['total_rows'] * 100
            )
        
        # Consolidar erros mais comuns
        for error_type, count in result.get('error_summary', {}).items():
            if error_type not in summary['most_common_errors']:
                summary['most_common_errors'][error_type] = 0
            summary['most_common_errors'][error_type] += count
        
        # Consolidar tempos de processamento
        summary['processing_summary']['total_time'] += result.get('processing_time', 0)
        summary['processing_summary']['total_chunks'] += result.get('chunks_processed', 0)
    
    # Calcular taxa de sucesso geral
    if summary['total_rows'] > 0:
        summary['overall_success_rate'] = (
            summary['total_valid'] / summary['total_rows'] * 100
        )
    
    # Calcular tempo médio por chunk
    if summary['processing_summary']['total_chunks'] > 0:
        summary['processing_summary']['avg_time_per_chunk'] = (
            summary['processing_summary']['total_time'] / 
            summary['processing_summary']['total_chunks']
        )
    
    summary['entities_validated'] = list(summary['entities_validated'])
    
    # Ordenar erros mais comuns
    summary['most_common_errors'] = dict(
        sorted(summary['most_common_errors'].items(), 
               key=lambda x: x[1], reverse=True)[:10]
    )
    
    return summary


def generate_batch_report(summary: Dict[str, Any]) -> str:
    """
    Gera relatório de validação em lote.
    
    Args:
        summary: Resumo da validação
        
    Returns:
        str: Relatório formatado
    """
    report = []
    report.append("=" * 60)
    report.append("RELATÓRIO DE VALIDAÇÃO EM LOTE")
    report.append("=" * 60)
    
    # Resumo geral
    report.append(f"Total de validações: {summary['total_validations']}")
    report.append(f"Entidades validadas: {', '.join(summary['entities_validated'])}")
    report.append(f"Total de linhas: {summary['total_rows']:,}")
    report.append(f"Linhas válidas: {summary['total_valid']:,}")
    report.append(f"Linhas inválidas: {summary['total_invalid']:,}")
    
    if summary['total_corrected'] > 0:
        report.append(f"Linhas corrigidas: {summary['total_corrected']:,}")
    
    report.append(f"Taxa de sucesso geral: {summary['overall_success_rate']:.1f}%")
    
    # Resumo por entidade
    if summary['by_entity']:
        report.append("\n" + "=" * 40)
        report.append("RESUMO POR ENTIDADE")
        report.append("=" * 40)
        
        for entity_type, entity_data in summary['by_entity'].items():
            report.append(f"\n{entity_type.upper()}:")
            report.append(f"  Total: {entity_data['total_rows']:,} linhas")
            report.append(f"  Válidas: {entity_data['valid_rows']:,}")
            report.append(f"  Inválidas: {entity_data['invalid_rows']:,}")
            if entity_data['corrected_rows'] > 0:
                report.append(f"  Corrigidas: {entity_data['corrected_rows']:,}")
            report.append(f"  Taxa de sucesso: {entity_data['success_rate']:.1f}%")
    
    # Erros mais comuns
    if summary['most_common_errors']:
        report.append("\n" + "=" * 40)
        report.append("ERROS MAIS COMUNS")
        report.append("=" * 40)
        
        for error_type, count in summary['most_common_errors'].items():
            report.append(f"  {error_type}: {count:,} ocorrências")
    
    # Resumo de performance
    processing = summary['processing_summary']
    if processing['total_time'] > 0:
        report.append("\n" + "=" * 40)
        report.append("PERFORMANCE")
        report.append("=" * 40)
        report.append(f"  Tempo total: {processing['total_time']:.2f}s")
        report.append(f"  Chunks processados: {processing['total_chunks']:,}")
        report.append(f"  Tempo médio por chunk: {processing['avg_time_per_chunk']:.3f}s")
        
        if summary['total_rows'] > 0:
            rows_per_second = summary['total_rows'] / processing['total_time']
            report.append(f"  Linhas por segundo: {rows_per_second:,.0f}")
    
    return "\n".join(report)


def optimize_dataframe_for_validation(df: pl.DataFrame, entity_type: str) -> pl.DataFrame:
    """
    Otimiza DataFrame para validação mais eficiente.
    
    Args:
        df: DataFrame para otimizar
        entity_type: Tipo da entidade
        
    Returns:
        pl.DataFrame: DataFrame otimizado
    """
    logger.info(f"Otimizando DataFrame {entity_type} para validação")
    
    # Remover colunas completamente vazias
    non_empty_columns = []
    for col in df.columns:
        if not df[col].is_null().all():
            non_empty_columns.append(col)
    
    if len(non_empty_columns) < len(df.columns):
        removed_count = len(df.columns) - len(non_empty_columns)
        logger.info(f"Removidas {removed_count} colunas vazias")
        df = df.select(non_empty_columns)
    
    # Otimizar tipos de dados
    optimized_columns = []
    for col in df.columns:
        col_data = df[col]
        
        # Se coluna é string mas contém apenas números, converter
        if col_data.dtype == pl.Utf8:
            # Verificar se todos os valores não-nulos são numéricos
            non_null_values = col_data.drop_nulls()
            if non_null_values.len() > 0:
                # Tentar converter para inteiro se possível
                try:
                    numeric_col = non_null_values.cast(pl.Int64, strict=False)
                    if not numeric_col.is_null().any():
                        optimized_columns.append(col_data.cast(pl.Int64, strict=False).alias(col))
                        continue
                except:
                    pass
        
        optimized_columns.append(col_data)
    
    if optimized_columns:
        df = df.with_columns(optimized_columns)
        logger.info("Tipos de dados otimizados")
    
    return df 