#!/usr/bin/env python
"""
Script para gerenciar o cache de downloads.

Uso:
  python cache_manager.py clear-cache  # Limpa todo o cache
  python cache_manager.py cache-info   # Exibe informações sobre o cache
"""
from src.utils.cli import main

if __name__ == "__main__":
    main() 