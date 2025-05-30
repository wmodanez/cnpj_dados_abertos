import os
import time
import json
import logging
logger = logging.getLogger(__name__)

class ProcessingCache:
    """Cache inteligente para otimizar reprocessamento."""
    def __init__(self):
        self.cache_dir = os.path.join(os.getenv('PATH_ZIP', 'data'), '.processing_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_file = os.path.join(self.cache_dir, 'processing_cache.json')
        self._load_cache()
    def _load_cache(self):
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
            else:
                self.cache = {}
        except Exception as e:
            logger.warning(f"Erro ao carregar cache de processamento: {e}")
            self.cache = {}
    def _save_cache(self):
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            logger.warning(f"Erro ao salvar cache de processamento: {e}")
    def is_processed(self, filename: str, file_size: int, file_mtime: int) -> bool:
        key = f"{filename}_{file_size}_{file_mtime}"
        return key in self.cache and self.cache[key].get('status') == 'completed'
    def mark_completed(self, filename: str, file_size: int, file_mtime: int, output_path: str):
        key = f"{filename}_{file_size}_{file_mtime}"
        self.cache[key] = {
            'status': 'completed',
            'output_path': output_path,
            'timestamp': time.time()
        }
        self._save_cache()

# Instância global do cache de processamento
processing_cache = ProcessingCache()
