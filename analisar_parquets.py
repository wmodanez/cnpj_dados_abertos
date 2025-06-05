#!/usr/bin/env python3
"""
Script para analisar os arquivos parquet de natureza jurídica e qualificação dos sócios
"""

import polars as pl
from pathlib import Path

def analisar_natureza_juridica():
    """Analisa o arquivo parquet de natureza jurídica."""
    print("🏢 ANÁLISE: NATUREZA JURÍDICA")
    print("=" * 50)
    
    try:
        df = pl.read_parquet('parquet/base/natureza_juridica.parquet')
        
        print(f"📊 INFORMAÇÕES BÁSICAS:")
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {df.columns}")
        print(f"  Dtypes: {df.dtypes}")
        
        print(f"\n📈 ESTATÍSTICAS:")
        print(f"  Total de registros: {len(df)}")
        print(f"  Códigos únicos: {df['codigo'].n_unique()}")
        print(f"  Menor código: {df['codigo'].min()}")
        print(f"  Maior código: {df['codigo'].max()}")
        
        print(f"\n📋 AMOSTRA DOS DADOS:")
        print(df.head(10))
        
        # Análise de categorias importantes
        print(f"\n🏛️ NATUREZAS IMPORTANTES:")
        importantes = df.filter(
            pl.col("codigo").is_in([0, 1244, 2054, 2062, 2135, 3999, 2305])
        ).sort("codigo")
        for row in importantes.to_dicts():
            print(f"  {row['codigo']:4d} - {row['descricao']}")
        
        # Análise por faixas de código
        print(f"\n📊 ANÁLISE POR FAIXAS:")
        print(f"  Códigos 1000-1999: {len(df.filter((pl.col('codigo') >= 1000) & (pl.col('codigo') < 2000)))}")
        print(f"  Códigos 2000-2999: {len(df.filter((pl.col('codigo') >= 2000) & (pl.col('codigo') < 3000)))}")
        print(f"  Códigos 3000-3999: {len(df.filter((pl.col('codigo') >= 3000) & (pl.col('codigo') < 4000)))}")
        print(f"  Códigos 4000+: {len(df.filter(pl.col('codigo') >= 4000))}")
        
    except Exception as e:
        print(f"❌ Erro ao analisar natureza jurídica: {e}")

def analisar_qualificacao_socios():
    """Analisa o arquivo parquet de qualificação dos sócios."""
    print("\n👥 ANÁLISE: QUALIFICAÇÃO DOS SÓCIOS")
    print("=" * 50)
    
    try:
        df = pl.read_parquet('parquet/base/qualificacao_socios.parquet')
        
        print(f"📊 INFORMAÇÕES BÁSICAS:")
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {df.columns}")
        print(f"  Dtypes: {df.dtypes}")
        
        print(f"\n📈 ESTATÍSTICAS:")
        print(f"  Total de registros: {len(df)}")
        print(f"  Códigos únicos: {df['codigo'].n_unique()}")
        print(f"  Menor código: {df['codigo'].min()}")
        print(f"  Maior código: {df['codigo'].max()}")
        
        print(f"\n📋 AMOSTRA DOS DADOS:")
        print(df.head(10))
        
        # Análise por categorias
        print(f"\n📊 ANÁLISE POR CATEGORIAS:")
        
        # Contar sócios
        socios = df.filter(pl.col("descricao").str.contains("(?i)s[óo]cio"))
        print(f"  Tipos de Sócios: {len(socios)}")
        for row in socios.head(5).to_dicts():
            print(f"    {row['codigo']:2d} - {row['descricao']}")
        
        # Contar administradores/diretores
        admins = df.filter(
            pl.col("descricao").str.contains("(?i)(administrador|diretor|presidente)")
        )
        print(f"\n  Cargos Administrativos: {len(admins)}")
        for row in admins.head(5).to_dicts():
            print(f"    {row['codigo']:2d} - {row['descricao']}")
        
        # Contar estrangeiros
        exterior = df.filter(pl.col("descricao").str.contains("(?i)exterior"))
        print(f"\n  Relacionados ao Exterior: {len(exterior)}")
        for row in exterior.head(3).to_dicts():
            print(f"    {row['codigo']:2d} - {row['descricao']}")
        
    except Exception as e:
        print(f"❌ Erro ao analisar qualificação dos sócios: {e}")

if __name__ == "__main__":
    analisar_natureza_juridica()
    analisar_qualificacao_socios() 