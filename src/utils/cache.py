"""
Utilitários para gerenciamento de cache de downloads.
"""
import os
import json
import logging
import datetime
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class DownloadCache:
    """
    Classe para gerenciar cache de arquivos baixados.
    Mantém um registro de arquivos já baixados para evitar downloads repetidos.
    """
    
    def __init__(self, cache_file: str):
        """
        Inicializa o cache de downloads.
        
        Args:
            cache_file: Caminho para o arquivo de cache
        """
        self.cache_file = cache_file
        self.cache_data = self._load_cache()
    
    def _load_cache(self) -> Dict[str, Any]:
        """
        Carrega os dados do cache do arquivo.
        
        Returns:
            Dicionário com os dados do cache
        """
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    cache_data = json.load(f)
                    logger.info(f"Cache carregado com {len(cache_data.get('files', []))} arquivos")
                    return cache_data
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Erro ao carregar arquivo de cache: {str(e)}. Criando novo cache.")
                return {"files": {}, "last_update": datetime.datetime.now().isoformat()}
        return {"files": {}, "last_update": datetime.datetime.now().isoformat()}
    
    def _save_cache(self) -> bool:
        """
        Salva os dados do cache no arquivo.
        
        Returns:
            True se o cache foi salvo com sucesso, False caso contrário
        """
        try:
            # Cria o diretório do cache se não existir
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            
            # Atualiza a data da última atualização
            self.cache_data["last_update"] = datetime.datetime.now().isoformat()
            
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache_data, f, indent=2)
            logger.info(f"Cache salvo com {len(self.cache_data.get('files', []))} arquivos")
            return True
        except IOError as e:
            logger.error(f"Erro ao salvar arquivo de cache: {str(e)}")
            return False
    
    def is_file_cached(self, filename: str, remote_size: int, remote_modified: int) -> bool:
        """
        Verifica se um arquivo está no cache e é atual.
        
        Args:
            filename: Nome do arquivo
            remote_size: Tamanho do arquivo remoto em bytes
            remote_modified: Data de modificação do arquivo remoto (timestamp)
            
        Returns:
            True se o arquivo está em cache e é atual, False caso contrário
        """
        files = self.cache_data.get("files", {})
        if filename in files:
            file_info = files[filename]
            # Verifica se o tamanho e a data de modificação são iguais
            if (file_info.get("size") == remote_size and 
                file_info.get("modified") == remote_modified):
                logger.info(f"Arquivo {filename} encontrado no cache e está atualizado")
                return True
            else:
                logger.info(f"Arquivo {filename} encontrado no cache mas está desatualizado")
                return False
        logger.info(f"Arquivo {filename} não encontrado no cache")
        return False
    
    def update_file_cache(self, filename: str, size: int, modified: int, status: str = "success") -> bool:
        """
        Atualiza as informações de um arquivo no cache.
        
        Args:
            filename: Nome do arquivo
            size: Tamanho do arquivo em bytes
            modified: Data de modificação do arquivo (timestamp)
            status: Status do download (success, failed, etc)
            
        Returns:
            True se o cache foi atualizado e salvo com sucesso, False caso contrário
        """
        if "files" not in self.cache_data:
            self.cache_data["files"] = {}
        
        self.cache_data["files"][filename] = {
            "size": size,
            "modified": modified,
            "last_download": datetime.datetime.now().isoformat(),
            "status": status
        }
        
        return self._save_cache()
    
    def remove_file_from_cache(self, filename: str) -> bool:
        """
        Remove um arquivo do cache.
        
        Args:
            filename: Nome do arquivo a ser removido
            
        Returns:
            True se o arquivo foi removido e o cache salvo com sucesso, False caso contrário
        """
        if "files" in self.cache_data and filename in self.cache_data["files"]:
            del self.cache_data["files"][filename]
            logger.info(f"Arquivo {filename} removido do cache")
            return self._save_cache()
        return False
    
    def clear_cache(self) -> bool:
        """
        Limpa todo o cache.
        
        Returns:
            True se o cache foi limpo e salvo com sucesso, False caso contrário
        """
        self.cache_data = {"files": {}, "last_update": datetime.datetime.now().isoformat()}
        logger.info("Cache limpo completamente")
        return self._save_cache()
    
    def get_cached_files(self) -> List[str]:
        """
        Retorna a lista de arquivos no cache.
        
        Returns:
            Lista com os nomes dos arquivos no cache
        """
        return list(self.cache_data.get("files", {}).keys())
    
    def get_file_info(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Retorna as informações de um arquivo no cache.
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            Dicionário com as informações do arquivo ou None se não estiver no cache
        """
        files = self.cache_data.get("files", {})
        return files.get(filename) 