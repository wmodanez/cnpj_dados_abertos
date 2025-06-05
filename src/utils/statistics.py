import time
import logging
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import json

logger = logging.getLogger(__name__)

@dataclass
class DownloadStats:
    """Estatísticas de download de um arquivo individual"""
    filename: str
    url: str
    size_bytes: int
    start_time: float
    end_time: float
    success: bool
    error: Optional[str] = None
    skip_reason: Optional[str] = None
    
    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def speed_mbps(self) -> float:
        if self.duration_seconds > 0 and self.success:
            return (self.size_bytes / (1024 * 1024)) / self.duration_seconds
        return 0.0

@dataclass
class ProcessingStats:
    """Estatísticas de processamento de um arquivo individual"""
    filename: str
    file_type: str  # empresas, estabelecimentos, simples, socios
    size_bytes: int
    start_time: float
    end_time: float
    success: bool
    error: Optional[str] = None
    records_processed: int = 0
    
    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def records_per_second(self) -> float:
        if self.duration_seconds > 0 and self.records_processed > 0:
            return self.records_processed / self.duration_seconds
        return 0.0

@dataclass
class DatabaseStats:
    """Estatísticas de criação do banco de dados"""
    start_time: float
    end_time: float
    success: bool
    tables_created: int = 0
    total_records: int = 0
    database_size_bytes: int = 0
    error: Optional[str] = None
    
    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time

class ProcessingStatistics:
    """Classe principal para coletar e gerenciar estatísticas do processamento"""
    
    def __init__(self):
        self.session_start_time = time.time()
        self.download_stats: List[DownloadStats] = []
        self.processing_stats: List[ProcessingStats] = []
        self.database_stats: Optional[DatabaseStats] = None
        self.total_start_time: Optional[float] = None
        self.total_end_time: Optional[float] = None
        
    def start_session(self):
        """Inicia uma nova sessão de estatísticas"""
        self.total_start_time = time.time()
        logger.info("Iniciando coleta de estatísticas de processamento")
    
    def end_session(self):
        """Finaliza a sessão de estatísticas"""
        self.total_end_time = time.time()
        logger.info("Finalizando coleta de estatísticas de processamento")
    
    def reset(self):
        """Reseta todas as estatísticas para uma nova sessão"""
        self.session_start_time = time.time()
        self.download_stats = []
        self.processing_stats = []
        self.database_stats = None
        self.total_start_time = None
        self.total_end_time = None
        logger.info("Estatísticas resetadas para nova sessão")
    
    def add_download_stat(self, filename: str, url: str, size_bytes: int, 
                         start_time: float, end_time: float, success: bool,
                         error: Optional[str] = None, skip_reason: Optional[str] = None):
        """Adiciona estatística de download"""
        stat = DownloadStats(
            filename=filename,
            url=url,
            size_bytes=size_bytes,
            start_time=start_time,
            end_time=end_time,
            success=success,
            error=error,
            skip_reason=skip_reason
        )
        self.download_stats.append(stat)
        logger.debug(f"Adicionada estatística de download para {filename}")
    
    def add_processing_stat(self, filename: str, file_type: str, size_bytes: int,
                           start_time: float, end_time: float, success: bool,
                           error: Optional[str] = None, records_processed: int = 0):
        """Adiciona estatística de processamento"""
        stat = ProcessingStats(
            filename=filename,
            file_type=file_type,
            size_bytes=size_bytes,
            start_time=start_time,
            end_time=end_time,
            success=success,
            error=error,
            records_processed=records_processed
        )
        self.processing_stats.append(stat)
        logger.debug(f"Adicionada estatística de processamento para {filename}")
    
    def set_database_stats(self, start_time: float, end_time: float, success: bool,
                          tables_created: int = 0, total_records: int = 0,
                          database_size_bytes: int = 0, error: Optional[str] = None):
        """Define estatísticas do banco de dados"""
        self.database_stats = DatabaseStats(
            start_time=start_time,
            end_time=end_time,
            success=success,
            tables_created=tables_created,
            total_records=total_records,
            database_size_bytes=database_size_bytes,
            error=error
        )
        logger.debug("Adicionadas estatísticas do banco de dados")
    
    def get_download_summary(self) -> Dict[str, Any]:
        """Retorna resumo das estatísticas de download"""
        if not self.download_stats:
            return {}
        
        successful_downloads = [s for s in self.download_stats if s.success]
        failed_downloads = [s for s in self.download_stats if not s.success and not s.skip_reason]
        skipped_downloads = [s for s in self.download_stats if s.skip_reason]
        
        # Calcular estatísticas apenas para downloads bem-sucedidos
        if successful_downloads:
            durations = [s.duration_seconds for s in successful_downloads]
            speeds = [s.speed_mbps for s in successful_downloads]
            sizes = [s.size_bytes for s in successful_downloads]
            
            return {
                'total_files': len(self.download_stats),
                'successful': len(successful_downloads),
                'failed': len(failed_downloads),
                'skipped': len(skipped_downloads),
                'total_size_mb': sum(sizes) / (1024 * 1024),
                'total_duration': sum(durations),
                'average_duration': sum(durations) / len(durations),
                'max_duration': max(durations),
                'min_duration': min(durations),
                'average_speed_mbps': sum(speeds) / len(speeds),
                'max_speed_mbps': max(speeds),
                'min_speed_mbps': min(speeds),
                'largest_file_mb': max(sizes) / (1024 * 1024),
                'smallest_file_mb': min(sizes) / (1024 * 1024),
            }
        
        return {
            'total_files': len(self.download_stats),
            'successful': 0,
            'failed': len(failed_downloads),
            'skipped': len(skipped_downloads),
        }
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Retorna resumo das estatísticas de processamento"""
        if not self.processing_stats:
            return {}
        
        successful_processing = [s for s in self.processing_stats if s.success]
        failed_processing = [s for s in self.processing_stats if not s.success]
        
        # Agrupar por tipo de arquivo
        by_type = {}
        for stat in self.processing_stats:
            if stat.file_type not in by_type:
                by_type[stat.file_type] = []
            by_type[stat.file_type].append(stat)
        
        type_summaries = {}
        for file_type, stats in by_type.items():
            successful = [s for s in stats if s.success]
            if successful:
                durations = [s.duration_seconds for s in successful]
                sizes = [s.size_bytes for s in successful]
                records = [s.records_processed for s in successful if s.records_processed > 0]
                
                type_summaries[file_type] = {
                    'total_files': len(stats),
                    'successful': len(successful),
                    'failed': len(stats) - len(successful),
                    'total_size_mb': sum(sizes) / (1024 * 1024),
                    'total_duration': sum(durations),
                    'average_duration': sum(durations) / len(durations),
                    'max_duration': max(durations),
                    'min_duration': min(durations),
                    'total_records': sum(records) if records else 0,
                }
        
        if successful_processing:
            durations = [s.duration_seconds for s in successful_processing]
            sizes = [s.size_bytes for s in successful_processing]
            
            return {
                'total_files': len(self.processing_stats),
                'successful': len(successful_processing),
                'failed': len(failed_processing),
                'total_size_mb': sum(sizes) / (1024 * 1024),
                'total_duration': sum(durations),
                'average_duration': sum(durations) / len(durations),
                'max_duration': max(durations),
                'min_duration': min(durations),
                'largest_file_mb': max(sizes) / (1024 * 1024),
                'by_type': type_summaries,
            }
        
        return {
            'total_files': len(self.processing_stats),
            'successful': 0,
            'failed': len(failed_processing),
            'by_type': type_summaries,
        }
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Retorna resumo das estatísticas do banco de dados"""
        if not self.database_stats:
            return {}
        
        return {
            'success': self.database_stats.success,
            'duration': self.database_stats.duration_seconds,
            'tables_created': self.database_stats.tables_created,
            'total_records': self.database_stats.total_records,
            'database_size_mb': self.database_stats.database_size_bytes / (1024 * 1024),
            'error': self.database_stats.error,
        }
    
    def get_overall_summary(self) -> Dict[str, Any]:
        """Retorna resumo geral de todas as estatísticas"""
        total_duration = 0
        if self.total_start_time and self.total_end_time:
            total_duration = self.total_end_time - self.total_start_time
        
        return {
            'session_start': datetime.fromtimestamp(self.session_start_time).isoformat(),
            'total_duration': total_duration,
            'download_summary': self.get_download_summary(),
            'processing_summary': self.get_processing_summary(),
            'database_summary': self.get_database_summary(),
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """Retorna resumo simplificado para uso no pipeline"""
        download_summary = self.get_download_summary()
        processing_summary = self.get_processing_summary()
        
        return {
            'total_files': download_summary.get('total_files', 0) + processing_summary.get('total_files', 0),
            'total_size_mb': download_summary.get('total_size_mb', 0) + processing_summary.get('total_size_mb', 0),
            'successful_downloads': download_summary.get('successful', 0),
            'failed_downloads': download_summary.get('failed', 0),
            'successful_processing': processing_summary.get('successful', 0),
            'failed_processing': processing_summary.get('failed', 0),
        }
    
    def print_detailed_report(self):
        """Imprime relatório detalhado das estatísticas"""
        print("\n" + "=" * 80)
        print("📊 RELATÓRIO DETALHADO DE ESTATÍSTICAS")
        print("=" * 80)
        
        # Resumo geral
        overall = self.get_overall_summary()
        print(f"\n🕐 Duração total da sessão: {self._format_duration(overall['total_duration'])}")
        print(f"📅 Início da sessão: {overall['session_start']}")
        
        # Estatísticas de download
        download_summary = overall['download_summary']
        if download_summary:
            print(f"\n📥 ESTATÍSTICAS DE DOWNLOAD:")
            print(f"   • Total de arquivos: {download_summary['total_files']}")
            print(f"   • Downloads bem-sucedidos: {download_summary['successful']}")
            print(f"   • Downloads falharam: {download_summary['failed']}")
            print(f"   • Arquivos pulados: {download_summary['skipped']}")
            
            if download_summary['successful'] > 0:
                print(f"   • Tamanho total baixado: {download_summary['total_size_mb']:.1f} MB")
                print(f"   • Tempo total de download: {self._format_duration(download_summary['total_duration'])}")
                print(f"   • Tempo médio por arquivo: {self._format_duration(download_summary['average_duration'])}")
                print(f"   • Maior tempo de download: {self._format_duration(download_summary['max_duration'])}")
                print(f"   • Menor tempo de download: {self._format_duration(download_summary['min_duration'])}")
                print(f"   • Velocidade média: {download_summary['average_speed_mbps']:.2f} MB/s")
                print(f"   • Velocidade máxima: {download_summary['max_speed_mbps']:.2f} MB/s")
                print(f"   • Velocidade mínima: {download_summary['min_speed_mbps']:.2f} MB/s")
                print(f"   • Maior arquivo: {download_summary['largest_file_mb']:.1f} MB")
                print(f"   • Menor arquivo: {download_summary['smallest_file_mb']:.1f} MB")
        
        # Estatísticas de processamento
        processing_summary = overall['processing_summary']
        if processing_summary:
            print(f"\n⚙️  ESTATÍSTICAS DE PROCESSAMENTO:")
            print(f"   • Total de arquivos: {processing_summary['total_files']}")
            print(f"   • Processamentos bem-sucedidos: {processing_summary['successful']}")
            print(f"   • Processamentos falharam: {processing_summary['failed']}")
            
            if processing_summary['successful'] > 0:
                print(f"   • Tamanho total processado: {processing_summary['total_size_mb']:.1f} MB")
                print(f"   • Tempo total de processamento: {self._format_duration(processing_summary['total_duration'])}")
                print(f"   • Tempo médio por arquivo: {self._format_duration(processing_summary['average_duration'])}")
                print(f"   • Maior tempo de processamento: {self._format_duration(processing_summary['max_duration'])}")
                print(f"   • Menor tempo de processamento: {self._format_duration(processing_summary['min_duration'])}")
                print(f"   • Maior arquivo processado: {processing_summary['largest_file_mb']:.1f} MB")
                
                # Estatísticas por tipo
                print(f"\n   📋 Por tipo de arquivo:")
                for file_type, type_stats in processing_summary['by_type'].items():
                    print(f"      {file_type.upper()}:")
                    print(f"         - Arquivos: {type_stats['successful']}/{type_stats['total_files']}")
                    print(f"         - Tamanho: {type_stats['total_size_mb']:.1f} MB")
                    print(f"         - Tempo total: {self._format_duration(type_stats['total_duration'])}")
                    print(f"         - Tempo médio: {self._format_duration(type_stats['average_duration'])}")
                    if type_stats['total_records'] > 0:
                        print(f"         - Registros processados: {type_stats['total_records']:,}")
        
        # Estatísticas do banco de dados
        database_summary = overall['database_summary']
        if database_summary:
            print(f"\n🗄️  ESTATÍSTICAS DO BANCO DE DADOS:")
            print(f"   • Status: {'✅ Sucesso' if database_summary['success'] else '❌ Falha'}")
            print(f"   • Tempo de criação: {self._format_duration(database_summary['duration'])}")
            print(f"   • Tabelas criadas: {database_summary['tables_created']}")
            print(f"   • Total de registros: {database_summary['total_records']:,}")
            print(f"   • Tamanho do banco: {database_summary['database_size_mb']:.1f} MB")
            if database_summary['error']:
                print(f"   • Erro: {database_summary['error']}")
        
        print("\n" + "=" * 80)
    
    def _format_duration(self, seconds: float) -> str:
        """Formata duração em segundos para formato legível"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = seconds % 60
            return f"{minutes}m {secs:.1f}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = seconds % 60
            return f"{hours}h {minutes}m {secs:.1f}s"
    
    def save_to_json(self, filename: Optional[str] = None) -> str:
        """Salva as estatísticas em arquivo JSON"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"logs/estatisticas_cnpj_{timestamp}.json"
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Preparar dados para JSON
        data = {
            'session_start': datetime.fromtimestamp(self.session_start_time).isoformat() if self.session_start_time else None,
            'session_end': datetime.fromtimestamp(self.total_end_time).isoformat() if self.total_end_time else None,
            'total_duration': self.total_end_time - self.session_start_time if self.session_start_time and self.total_end_time else 0,
            'download_summary': self.get_download_summary(),
            'processing_summary': self.get_processing_summary(),
            'database_summary': self.get_database_summary()
        }
        
        # Salvar JSON
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Gerar automaticamente o relatório Markdown
        self._generate_markdown_report(filename)
        
        return filename
    
    def _generate_markdown_report(self, json_filename: str) -> str:
        """Gera automaticamente um relatório Markdown baseado no JSON"""
        try:
            # Importar o módulo de conversão
            from .json_to_markdown import convert_json_to_markdown
            
            # Gerar o relatório Markdown
            markdown_filename = convert_json_to_markdown(json_filename)
            print(f"📄 Relatório Markdown gerado: {markdown_filename}")
            return markdown_filename
            
        except ImportError:
            print("⚠️  Módulo json_to_markdown não encontrado. Relatório Markdown não gerado.")
            return ""
        except Exception as e:
            print(f"⚠️  Erro ao gerar relatório Markdown: {e}")
            return ""

# Instância global para coletar estatísticas
global_stats = ProcessingStatistics() 