#!/usr/bin/env python
"""
Script para gerenciar o cache de downloads.

Uso:
  python -m src.cache_manager clear-cache  # Limpa todo o cache
  python -m src.cache_manager cache-info   # Exibe informações sobre o cache
"""
# Ajustado para import relativo dentro do pacote src
from .utils.cli import main

if __name__ == "__main__":
    main()
