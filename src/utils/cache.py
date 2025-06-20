"""
Utilitários para gerenciamento de cache de downloads.
"""
import datetime
import json
import logging
import os
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class DownloadCache:
    """
    Classe para gerenciar cache de arquivos baixados.
    Mantém um registro de arquivos já baixados para evitar downloads repetidos.
    """

    def __init__(self, cache_path: str):
        """
        Inicializa o cache de downloads.
        
        Args:
            cache_path: Caminho completo para o arquivo de cache
        """
        self.cache_path = cache_path
        self.cache_dir = os.path.dirname(cache_path)
        self.cache_data = self._load_cache()
        
        # Garante que o diretório do cache existe
        os.makedirs(self.cache_dir, exist_ok=True)
        
        logger.debug(f"Cache inicializado em: {cache_path}")
        logger.debug(f"Total de arquivos em cache: {len(self.cache_data.get('files', {}))}")
        logger.debug(f"Total de erros registrados: {len(self.cache_data.get('errors', {}))}")

    def _load_cache(self) -> Dict:
        """Carrega o cache do arquivo JSON."""
        try:
            if os.path.exists(self.cache_path):
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Garante que todas as chaves necessárias existem
                    if "files" not in data:
                        data["files"] = {}
                    if "errors" not in data:
                        data["errors"] = {}
                    if "processing" not in data:
                        data["processing"] = {}
                    return data
            return {"files": {}, "errors": {}, "processing": {}}
        except Exception as e:
            logger.error(f"Erro ao carregar cache: {e}")
            return {"files": {}, "errors": {}, "processing": {}}

    def _save_cache(self):
        """Salva o cache no arquivo JSON."""
        try:
            # Garante que o diretório existe
            os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
            
            with open(self.cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro ao salvar cache: {e}")

    def is_file_cached(self, filename: str, remote_size: int, remote_last_modified: int) -> bool:
        """
        Verifica se um arquivo está em cache e se está atualizado.
        
        Args:
            filename: Nome do arquivo
            remote_size: Tamanho do arquivo remoto
            remote_last_modified: Timestamp da última modificação do arquivo remoto
            
        Returns:
            bool: True se o arquivo está em cache e atualizado, False caso contrário
        """
        file_info = self.cache_data.get("files", {}).get(filename)
        if not file_info:
            return False
            
        # Verifica se o arquivo tem erros registrados
        if filename in self.cache_data.get("errors", {}):
            logger.debug(f"Arquivo {filename} tem erros registrados no cache")
            return False
            
        # Verifica se o tamanho e data de modificação correspondem
        return (file_info.get("size") == remote_size and 
                file_info.get("modified") == remote_last_modified)

    def update_file_cache(self, filename: str, size: int, modified: int, status: str = "success", log_update: bool = True):
        """
        Atualiza o cache com informações de um arquivo.
        
        Args:
            filename: Nome do arquivo
            size: Tamanho do arquivo
            modified: Timestamp da última modificação
            status: Status do arquivo (success, error, etc)
            log_update: Se True, loga a atualização do cache
        """
        try:
            if "files" not in self.cache_data:
                self.cache_data["files"] = {}
                
            self.cache_data["files"][filename] = {
                "size": size,
                "modified": modified,
                "status": status,
                "last_updated": datetime.datetime.now().isoformat()
            }
            
            # Se o arquivo foi atualizado com sucesso, remove qualquer erro registrado
            if status == "success":
                if filename in self.cache_data.get("errors", {}):
                    del self.cache_data["errors"][filename]
                # Remove também do processamento se existir
                if filename in self.cache_data.get("processing", {}):
                    del self.cache_data["processing"][filename]
                
            self._save_cache()
            if log_update:
                logger.debug(f"Cache atualizado para {filename}")
        except Exception as e:
            logger.error(f"Erro ao atualizar cache para {filename}: {e}")

    def update_processing_status(self, filename: str, status: str, parquet_path: str | None = None):
        """
        Atualiza o status de processamento de um arquivo.
        
        Args:
            filename: Nome do arquivo
            status: Status do processamento (downloading, extracting, processing, completed, error)
            parquet_path: Caminho do arquivo parquet gerado (se aplicável)
        """
        try:
            if "processing" not in self.cache_data:
                self.cache_data["processing"] = {}
                
            self.cache_data["processing"][filename] = {
                "status": status,
                "parquet_path": parquet_path,
                "last_updated": datetime.datetime.now().isoformat()
            }
            
            if status == "completed" and parquet_path:
                # Verifica se o arquivo parquet existe
                if os.path.exists(parquet_path):
                    self.cache_data["processing"][filename]["parquet_size"] = os.path.getsize(parquet_path)
                    self.cache_data["processing"][filename]["parquet_modified"] = int(os.path.getmtime(parquet_path))
            
            self._save_cache()
            logger.debug(f"Status de processamento atualizado para {filename}: {status}")
        except Exception as e:
            logger.error(f"Erro ao atualizar status de processamento para {filename}: {e}")

    def get_processing_status(self, filename: str) -> Dict[str, Any]:
        """
        Retorna o status de processamento de um arquivo.
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            Dict[str, Any]: Informações de processamento ou {} se não encontrado
        """
        return self.cache_data.get("processing", {}).get(filename, {})

    def is_file_processed(self, filename: str) -> bool:
        """
        Verifica se um arquivo já foi processado com sucesso.
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            bool: True se o arquivo foi processado com sucesso, False caso contrário
        """
        processing_info = self.get_processing_status(filename)
        if not processing_info:
            return False
            
        status = processing_info.get("status")
        if status != "completed":
            return False
            
        # Se tem parquet_path, verifica se o arquivo existe
        parquet_path = processing_info.get("parquet_path")
        if parquet_path and not os.path.exists(parquet_path):
            return False
            
        return True

    def remove_file_from_cache(self, filename: str):
        """
        Remove um arquivo do cache.
        
        Args:
            filename: Nome do arquivo
        """
        try:
            if filename in self.cache_data.get("files", {}):
                del self.cache_data["files"][filename]
            if filename in self.cache_data.get("errors", {}):
                del self.cache_data["errors"][filename]
            if filename in self.cache_data.get("processing", {}):
                del self.cache_data["processing"][filename]
            self._save_cache()
            logger.debug(f"Arquivo {filename} removido do cache")
        except Exception as e:
            logger.error(f"Erro ao remover {filename} do cache: {e}")

    def register_file_error(self, filename: str, error_msg: str):
        """
        Registra um erro para um arquivo no cache.
        
        Args:
            filename: Nome do arquivo
            error_msg: Mensagem de erro
        """
        try:
            if "errors" not in self.cache_data:
                self.cache_data["errors"] = {}
                
            self.cache_data["errors"][filename] = {
                "message": error_msg,
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            # Atualiza o status do arquivo para error
            if filename in self.cache_data.get("files", {}):
                self.cache_data["files"][filename]["status"] = "error"
            
            # Atualiza o status de processamento para error
            self.update_processing_status(filename, "error")
                
            self._save_cache()
            logger.debug(f"Erro registrado para {filename}: {error_msg}")
        except Exception as e:
            logger.error(f"Erro ao registrar erro para {filename}: {e}")

    def has_file_error(self, filename: str) -> bool:
        """
        Verifica se um arquivo tem erros registrados no cache.
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            bool: True se o arquivo tem erros, False caso contrário
        """
        return filename in self.cache_data.get("errors", {})

    def get_files_with_errors(self) -> List[str]:
        """
        Retorna a lista de arquivos com erros registrados no cache.
        
        Returns:
            List[str]: Lista de nomes de arquivos com erros
        """
        return list(self.cache_data.get("errors", {}).keys())

    def get_file_info(self, filename: str) -> Optional[Dict]:
        """
        Retorna as informações de um arquivo no cache.
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            Optional[Dict]: Informações do arquivo ou None se não encontrado
        """
        return self.cache_data.get("files", {}).get(filename)

    def clear_cache(self):
        """Limpa todo o cache."""
        try:
            self.cache_data = {"files": {}, "errors": {}, "processing": {}}
            self._save_cache()
            logger.info("Cache limpo com sucesso")
        except Exception as e:
            logger.error(f"Erro ao limpar cache: {e}")

    def get_cache_stats(self) -> Dict:
        """
        Retorna estatísticas do cache.
        
        Returns:
            Dict: Estatísticas do cache
        """
        stats = {
            "total_files": len(self.cache_data.get("files", {})),
            "total_errors": len(self.cache_data.get("errors", {})),
            "total_processing": len(self.cache_data.get("processing", {})),
            "files_by_status": {},
            "processing_by_status": {}
        }
        
        # Conta arquivos por status
        for file_info in self.cache_data.get("files", {}).values():
            status = file_info.get("status", "unknown")
            stats["files_by_status"][status] = stats["files_by_status"].get(status, 0) + 1
            
        # Conta processamentos por status
        for proc_info in self.cache_data.get("processing", {}).values():
            status = proc_info.get("status", "unknown")
            stats["processing_by_status"][status] = stats["processing_by_status"].get(status, 0) + 1
            
        return stats
