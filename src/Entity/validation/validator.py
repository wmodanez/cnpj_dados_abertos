"""
Validador principal de entidades usando Pydantic + validações customizadas.

Este módulo implementa a classe EntityValidator que combina:
- Schemas Pydantic para validação declarativa
- Validações customizadas para regras de negócio específicas
- Processamento otimizado para DataFrames grandes
- Correção automática de dados malformados
"""

from typing import Dict, Any, List, Type, Union, Optional, Tuple
import polars as pl
from pydantic import BaseModel, ValidationError
import logging
import re
from datetime import datetime
from ..schemas import (
    EmpresaSchema, EstabelecimentoSchema, 
    SocioSchema, SimplesSchema,
    get_schema_for_entity
)

logger = logging.getLogger(__name__)


class EntityValidator:
    """Sistema de validação híbrido usando Pydantic + validações customizadas"""
    
    # Mapeamento de entidades para schemas
    SCHEMA_MAPPING = {
        'empresa': EmpresaSchema,
        'estabelecimento': EstabelecimentoSchema,
        'socio': SocioSchema,
        'simples': SimplesSchema
    }
    
    def __init__(self, entity_type: str, strict_mode: bool = False):
        """
        Inicializa o validador.
        
        Args:
            entity_type: Tipo da entidade ('empresa', 'estabelecimento', etc.)
            strict_mode: Se deve usar modo estrito (rejeitar dados inválidos)
        """
        self.entity_type = entity_type.lower()
        self.strict_mode = strict_mode
        
        if self.entity_type not in self.SCHEMA_MAPPING:
            raise ValueError(f"Tipo de entidade inválido: {entity_type}")
        
        self.schema_class = self.SCHEMA_MAPPING[self.entity_type]
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{self.entity_type}")
        
        # Estatísticas de validação
        self.stats = {
            'total_rows': 0,
            'valid_rows': 0,
            'invalid_rows': 0,
            'corrected_rows': 0,
            'errors_by_type': {},
            'warnings': []
        }
    
    def validate_dataframe(self, df: pl.DataFrame, 
                          sample_size: Optional[int] = None,
                          return_details: bool = False) -> Dict[str, Any]:
        """
        Valida DataFrame usando schema Pydantic.
        
        Args:
            df: DataFrame para validar
            sample_size: Número de linhas para validar (None = todas)
            return_details: Se deve retornar detalhes dos erros
            
        Returns:
            Dict[str, Any]: Resultado da validação
        """
        self.logger.info(f"Iniciando validação de DataFrame {self.entity_type}")
        
        # Resetar estatísticas
        self._reset_stats()
        
        # Determinar amostra
        if sample_size and df.height > sample_size:
            sample_df = df.head(sample_size)
            self.logger.info(f"Validando amostra de {sample_size} linhas de {df.height} total")
        else:
            sample_df = df
            sample_size = df.height
        
        self.stats['total_rows'] = sample_df.height
        
        validation_results = {
            'entity_type': self.entity_type,
            'total_rows': df.height,
            'validated_rows': sample_df.height,
            'valid_rows': 0,
            'invalid_rows': 0,
            'corrected_rows': 0,
            'success_rate': 0.0,
            'errors': [],
            'warnings': [],
            'error_summary': {},
            'sample_valid_data': [],
            'sample_invalid_data': []
        }
        
        # Validar cada linha
        for i, row in enumerate(sample_df.iter_rows(named=True)):
            try:
                # Tentar validar com schema
                validated_data = self.schema_class(**row)
                validation_results['valid_rows'] += 1
                
                # Guardar amostra de dados válidos
                if len(validation_results['sample_valid_data']) < 5:
                    validation_results['sample_valid_data'].append(validated_data.dict())
                
            except ValidationError as e:
                validation_results['invalid_rows'] += 1
                
                # Processar erros de validação
                error_details = self._process_validation_error(e, row, i)
                validation_results['errors'].extend(error_details)
                
                # Contar tipos de erro
                for error in error_details:
                    error_type = error.get('type', 'unknown')
                    if error_type not in validation_results['error_summary']:
                        validation_results['error_summary'][error_type] = 0
                    validation_results['error_summary'][error_type] += 1
                
                # Guardar amostra de dados inválidos
                if len(validation_results['sample_invalid_data']) < 5:
                    validation_results['sample_invalid_data'].append({
                        'row_index': i,
                        'row_data': row,
                        'errors': error_details
                    })
                
            except Exception as e:
                validation_results['invalid_rows'] += 1
                error_detail = {
                    'row_index': i,
                    'field': 'general',
                    'message': str(e),
                    'type': 'unexpected_error',
                    'value': 'N/A'
                }
                validation_results['errors'].append(error_detail)
        
        # Calcular estatísticas finais
        validation_results['success_rate'] = (
            validation_results['valid_rows'] / validation_results['validated_rows'] * 100
            if validation_results['validated_rows'] > 0 else 0
        )
        
        # Gerar warnings baseados na taxa de sucesso
        validation_results['warnings'] = self._generate_warnings(validation_results)
        
        # Atualizar estatísticas internas
        self.stats.update({
            'valid_rows': validation_results['valid_rows'],
            'invalid_rows': validation_results['invalid_rows'],
            'errors_by_type': validation_results['error_summary']
        })
        
        self.logger.info(f"Validação concluída: {validation_results['success_rate']:.1f}% de sucesso")
        
        return validation_results
    
    def clean_dataframe(self, df: pl.DataFrame, 
                       remove_invalid: bool = True,
                       attempt_correction: bool = True) -> Tuple[pl.DataFrame, Dict[str, Any]]:
        """
        Remove ou corrige linhas inválidas do DataFrame.
        
        Args:
            df: DataFrame para limpar
            remove_invalid: Se deve remover linhas inválidas
            attempt_correction: Se deve tentar corrigir dados automaticamente
            
        Returns:
            Tuple[pl.DataFrame, Dict]: DataFrame limpo e relatório de limpeza
        """
        self.logger.info(f"Iniciando limpeza de DataFrame {self.entity_type} ({df.height} linhas)")
        
        valid_rows = []
        invalid_count = 0
        corrected_count = 0
        correction_details = []
        
        for i, row in enumerate(df.iter_rows(named=True)):
            try:
                # Tentar validar diretamente
                validated_data = self.schema_class(**row)
                valid_rows.append(validated_data.dict())
                
            except ValidationError as e:
                if attempt_correction:
                    # Tentar corrigir dados
                    corrected_row, corrections = self._attempt_correction(row, e)
                    
                    if corrected_row:
                        try:
                            # Validar dados corrigidos
                            validated_corrected = self.schema_class(**corrected_row)
                            valid_rows.append(validated_corrected.dict())
                            corrected_count += 1
                            
                            if corrections:
                                correction_details.append({
                                    'row_index': i,
                                    'corrections': corrections
                                })
                            
                        except ValidationError:
                            # Correção falhou
                            if not remove_invalid:
                                valid_rows.append(row)  # Manter dados originais
                            invalid_count += 1
                    else:
                        # Não foi possível corrigir
                        if not remove_invalid:
                            valid_rows.append(row)  # Manter dados originais
                        invalid_count += 1
                else:
                    # Não tentar correção
                    if not remove_invalid:
                        valid_rows.append(row)  # Manter dados originais
                    invalid_count += 1
                    
            except Exception as e:
                # Erro inesperado
                self.logger.error(f"Erro inesperado na linha {i}: {str(e)}")
                if not remove_invalid:
                    valid_rows.append(row)  # Manter dados originais
                invalid_count += 1
        
        # Criar DataFrame limpo
        if valid_rows:
            cleaned_df = pl.DataFrame(valid_rows)
        else:
            # Retornar DataFrame vazio com mesmas colunas
            cleaned_df = df.head(0)
        
        # Relatório de limpeza
        cleaning_report = {
            'original_rows': df.height,
            'cleaned_rows': len(valid_rows),
            'removed_rows': invalid_count if remove_invalid else 0,
            'corrected_rows': corrected_count,
            'correction_rate': corrected_count / df.height * 100 if df.height > 0 else 0,
            'success_rate': len(valid_rows) / df.height * 100 if df.height > 0 else 0,
            'correction_details': correction_details[:10]  # Primeiros 10 exemplos
        }
        
        self.logger.info(
            f"Limpeza concluída: {len(valid_rows)} linhas válidas, "
            f"{corrected_count} corrigidas, {invalid_count} removidas/mantidas"
        )
        
        return cleaned_df, cleaning_report
    
    def validate_single_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida uma única linha de dados.
        
        Args:
            row: Dicionário com dados da linha
            
        Returns:
            Dict[str, Any]: Resultado da validação
        """
        try:
            validated_data = self.schema_class(**row)
            return {
                'valid': True,
                'data': validated_data.dict(),
                'errors': [],
                'warnings': []
            }
            
        except ValidationError as e:
            errors = self._process_validation_error(e, row, 0)
            return {
                'valid': False,
                'data': None,
                'errors': errors,
                'warnings': []
            }
            
        except Exception as e:
            return {
                'valid': False,
                'data': None,
                'errors': [{'field': 'general', 'message': str(e), 'type': 'unexpected_error'}],
                'warnings': []
            }
    
    def get_validation_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas de validação.
        
        Returns:
            Dict[str, Any]: Estatísticas detalhadas
        """
        total = self.stats['total_rows']
        valid = self.stats['valid_rows']
        invalid = self.stats['invalid_rows']
        corrected = self.stats['corrected_rows']
        
        return {
            'entity_type': self.entity_type,
            'total_rows': total,
            'valid_rows': valid,
            'invalid_rows': invalid,
            'corrected_rows': corrected,
            'success_rate': valid / total * 100 if total > 0 else 0,
            'correction_rate': corrected / total * 100 if total > 0 else 0,
            'error_rate': invalid / total * 100 if total > 0 else 0,
            'errors_by_type': self.stats['errors_by_type'],
            'most_common_errors': self._get_most_common_errors()
        }
    
    def get_validation_report(self, validation_results: Dict[str, Any]) -> str:
        """Gera relatório de validação em formato texto."""
        report = []
        report.append(f"=== RELATÓRIO DE VALIDAÇÃO - {validation_results['entity_type'].upper()} ===")
        report.append(f"Total de linhas: {validation_results['total_rows']:,}")
        report.append(f"Linhas validadas: {validation_results['validated_rows']:,}")
        report.append(f"Linhas válidas: {validation_results['valid_rows']:,}")
        report.append(f"Linhas inválidas: {validation_results['invalid_rows']:,}")
        report.append(f"Taxa de sucesso: {validation_results['success_rate']:.1f}%")
        
        if validation_results.get('corrected_rows', 0) > 0:
            report.append(f"Linhas corrigidas: {validation_results['corrected_rows']:,}")
        
        if validation_results['warnings']:
            report.append("\n⚠️  AVISOS:")
            for warning in validation_results['warnings']:
                report.append(f"  • {warning}")
        
        if validation_results['error_summary']:
            report.append("\n❌ RESUMO DE ERROS:")
            sorted_errors = sorted(
                validation_results['error_summary'].items(), 
                key=lambda x: x[1], reverse=True
            )
            for error_type, count in sorted_errors[:10]:  # Top 10
                report.append(f"  • {error_type}: {count:,} ocorrências")
        
        if validation_results['sample_invalid_data']:
            report.append("\n🔍 AMOSTRAS DE DADOS INVÁLIDOS:")
            for i, sample in enumerate(validation_results['sample_invalid_data'][:3]):
                report.append(f"  Exemplo {i+1} (linha {sample['row_index']}):")
                for error in sample['errors'][:3]:  # Primeiros 3 erros
                    report.append(f"    - {error['field']}: {error['message']}")
        
        return "\n".join(report)
    
    # Métodos privados de apoio
    
    def _reset_stats(self):
        """Reseta estatísticas de validação."""
        self.stats = {
            'total_rows': 0,
            'valid_rows': 0,
            'invalid_rows': 0,
            'corrected_rows': 0,
            'errors_by_type': {},
            'warnings': []
        }
    
    def _process_validation_error(self, error: ValidationError, 
                                 row: Dict[str, Any], row_index: int) -> List[Dict[str, Any]]:
        """
        Processa erro de validação do Pydantic.
        
        Args:
            error: Erro de validação
            row: Dados da linha
            row_index: Índice da linha
            
        Returns:
            List[Dict]: Lista de detalhes dos erros
        """
        error_details = []
        
        for pydantic_error in error.errors():
            field = pydantic_error['loc'][0] if pydantic_error['loc'] else 'unknown'
            message = pydantic_error['msg']
            error_type = pydantic_error['type']
            value = row.get(field, 'N/A')
            
            error_detail = {
                'row_index': row_index,
                'field': field,
                'message': message,
                'type': error_type,
                'value': value,
                'input_type': type(value).__name__
            }
            
            error_details.append(error_detail)
        
        return error_details
    
    def _attempt_correction(self, row: Dict[str, Any], 
                           error: ValidationError) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        """
        Tenta corrigir dados automaticamente.
        
        Args:
            row: Dados originais da linha
            error: Erro de validação
            
        Returns:
            Tuple[Dict, List]: Dados corrigidos e lista de correções aplicadas
        """
        # Importar correções específicas
        from .corrections import EntityCorrections
        
        corrector = EntityCorrections(self.entity_type)
        corrected_data, corrections = corrector.correct_data(row, error)
        
        # Tentar validar dados corrigidos
        if corrected_data:
            try:
                self.schema_class(**corrected_data)
                return corrected_data, corrections
            except ValidationError:
                return None, []
        
        return None, []
    
    def _generate_warnings(self, validation_results: Dict[str, Any]) -> List[str]:
        """Gera warnings baseados nos resultados da validação."""
        warnings = []
        success_rate = validation_results['success_rate']
        
        if success_rate < 50:
            warnings.append(
                f"Taxa de sucesso muito baixa ({success_rate:.1f}%) - "
                "verifique formato dos dados"
            )
        elif success_rate < 80:
            warnings.append(
                f"Taxa de sucesso moderada ({success_rate:.1f}%) - "
                "alguns dados podem estar inconsistentes"
            )
        
        # Warnings específicos por tipo de erro
        error_summary = validation_results.get('error_summary', {})
        
        if 'value_error' in error_summary and error_summary['value_error'] > 10:
            warnings.append("Muitos erros de valor - verifique formatação dos dados")
        
        if 'type_error' in error_summary and error_summary['type_error'] > 10:
            warnings.append("Muitos erros de tipo - verifique tipos de dados")
        
        return warnings
    
    def _get_most_common_errors(self) -> List[Tuple[str, int]]:
        """Retorna os erros mais comuns ordenados por frequência."""
        errors_by_type = self.stats['errors_by_type']
        return sorted(errors_by_type.items(), key=lambda x: x[1], reverse=True)[:5] 