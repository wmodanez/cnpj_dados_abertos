#!/usr/bin/env python3
"""
Utilitário para converter arquivos JSON de estatísticas em relatórios Markdown.
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, Any

def format_duration(seconds: float) -> str:
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

def format_size(size_mb: float) -> str:
    """Formata tamanho em MB para formato legível"""
    if size_mb < 1024:
        return f"{size_mb:.1f} MB"
    else:
        size_gb = size_mb / 1024
        return f"{size_gb:.2f} GB"

def format_number(number: int) -> str:
    """Formata números grandes com separadores de milhares"""
    return f"{number:,}".replace(",", ".")

def generate_markdown_report(json_data: Dict[str, Any]) -> str:
    """Gera relatório em Markdown baseado nos dados JSON"""
    
    # Cabeçalho do relatório
    session_start = json_data.get('session_start', 'N/A')
    total_duration = json_data.get('total_duration', 0)
    
    # Converter timestamp ISO para formato legível
    try:
        dt = datetime.fromisoformat(session_start.replace('Z', '+00:00'))
        formatted_date = dt.strftime('%d/%m/%Y às %H:%M:%S')
    except:
        formatted_date = session_start
    
    markdown = f"""# 📊 Relatório de Estatísticas - Processamento CNPJ

**Data da Execução:** {formatted_date}  
**Duração Total:** {format_duration(total_duration)}

---

"""

    # Estatísticas de Download
    download_summary = json_data.get('download_summary', {})
    if download_summary:
        markdown += """## 📥 Estatísticas de Download

"""
        
        total_files = download_summary.get('total_files', 0)
        successful = download_summary.get('successful', 0)
        failed = download_summary.get('failed', 0)
        skipped = download_summary.get('skipped', 0)
        
        success_pct = (successful/total_files*100) if total_files > 0 else 0
        failed_pct = (failed/total_files*100) if total_files > 0 else 0
        skipped_pct = (skipped/total_files*100) if total_files > 0 else 0
        
        markdown += f"""### 📊 Resumo Geral
- **Total de arquivos:** {total_files}
- **Downloads bem-sucedidos:** {successful} ({success_pct:.1f}%)
- **Downloads falharam:** {failed} ({failed_pct:.1f}%)
- **Arquivos pulados:** {skipped} ({skipped_pct:.1f}%)

"""
        
        if successful > 0:
            total_size_mb = download_summary.get('total_size_mb', 0)
            total_duration = download_summary.get('total_duration', 0)
            avg_duration = download_summary.get('average_duration', 0)
            max_duration = download_summary.get('max_duration', 0)
            min_duration = download_summary.get('min_duration', 0)
            avg_speed = download_summary.get('average_speed_mbps', 0)
            max_speed = download_summary.get('max_speed_mbps', 0)
            min_speed = download_summary.get('min_speed_mbps', 0)
            largest_file = download_summary.get('largest_file_mb', 0)
            smallest_file = download_summary.get('smallest_file_mb', 0)
            
            markdown += f"""### ⏱️ Tempos e Velocidades
- **Tamanho total baixado:** {format_size(total_size_mb)}
- **Tempo total de download:** {format_duration(total_duration)}
- **Tempo médio por arquivo:** {format_duration(avg_duration)}
- **Maior tempo de download:** {format_duration(max_duration)}
- **Menor tempo de download:** {format_duration(min_duration)}

### 🚀 Velocidades de Download
- **Velocidade média:** {avg_speed:.2f} MB/s
- **Velocidade máxima:** {max_speed:.2f} MB/s
- **Velocidade mínima:** {min_speed:.2f} MB/s

### 📦 Tamanhos dos Arquivos
- **Maior arquivo:** {format_size(largest_file)}
- **Menor arquivo:** {format_size(smallest_file)}

"""

    # Estatísticas de Processamento
    processing_summary = json_data.get('processing_summary', {})
    if processing_summary:
        markdown += """## ⚙️ Estatísticas de Processamento

"""
        
        total_files = processing_summary.get('total_files', 0)
        successful = processing_summary.get('successful', 0)
        failed = processing_summary.get('failed', 0)
        
        success_pct = (successful/total_files*100) if total_files > 0 else 0
        failed_pct = (failed/total_files*100) if total_files > 0 else 0
        
        markdown += f"""### 📊 Resumo Geral
- **Total de arquivos:** {total_files}
- **Processamentos bem-sucedidos:** {successful} ({success_pct:.1f}%)
- **Processamentos falharam:** {failed} ({failed_pct:.1f}%)

"""
        
        if successful > 0:
            total_size_mb = processing_summary.get('total_size_mb', 0)
            total_duration = processing_summary.get('total_duration', 0)
            avg_duration = processing_summary.get('average_duration', 0)
            max_duration = processing_summary.get('max_duration', 0)
            min_duration = processing_summary.get('min_duration', 0)
            largest_file = processing_summary.get('largest_file_mb', 0)
            
            markdown += f"""### ⏱️ Tempos de Processamento
- **Tamanho total processado:** {format_size(total_size_mb)}
- **Tempo total de processamento:** {format_duration(total_duration)}
- **Tempo médio por arquivo:** {format_duration(avg_duration)}
- **Maior tempo de processamento:** {format_duration(max_duration)}
- **Menor tempo de processamento:** {format_duration(min_duration)}
- **Maior arquivo processado:** {format_size(largest_file)}

"""
            
            # Estatísticas por tipo
            by_type = processing_summary.get('by_type', {})
            if by_type:
                markdown += """### 📋 Estatísticas por Tipo de Arquivo

| Tipo | Arquivos | Sucesso | Tamanho | Tempo Total | Tempo Médio |
|------|----------|---------|---------|-------------|-------------|
"""
                
                for file_type, stats in by_type.items():
                    total_files_type = stats.get('total_files', 0)
                    successful_type = stats.get('successful', 0)
                    size_mb = stats.get('total_size_mb', 0)
                    duration = stats.get('total_duration', 0)
                    avg_duration = stats.get('average_duration', 0)
                    
                    markdown += f"| **{file_type.upper()}** | {total_files_type} | {successful_type} | {format_size(size_mb)} | {format_duration(duration)} | {format_duration(avg_duration)} |\n"
                
                markdown += "\n"

    # Estatísticas do Banco de Dados
    database_summary = json_data.get('database_summary', {})
    if database_summary:
        markdown += """## 🗄️ Estatísticas do Banco de Dados

"""
        
        success = database_summary.get('success', False)
        duration = database_summary.get('duration', 0)
        tables_created = database_summary.get('tables_created', 0)
        total_records = database_summary.get('total_records', 0)
        database_size_mb = database_summary.get('database_size_mb', 0)
        error = database_summary.get('error')
        
        status_icon = "✅" if success else "❌"
        status_text = "Sucesso" if success else "Falha"
        
        markdown += f"""### 📊 Resumo do Banco
- **Status:** {status_icon} {status_text}
- **Tempo de criação:** {format_duration(duration)}
- **Tabelas criadas:** {tables_created}
- **Total de registros:** {format_number(total_records)}
- **Tamanho do banco:** {format_size(database_size_mb)}

"""
        
        if error:
            markdown += f"""### ❌ Erro Reportado
```
{error}
```

"""

    # Rodapé
    current_time = datetime.now().strftime('%d/%m/%Y às %H:%M:%S')
    markdown += f"""---

## 📝 Informações Técnicas

**Relatório gerado automaticamente pelo sistema de estatísticas do processamento CNPJ.**

### 🔧 Como foi gerado
Este relatório foi criado a partir do arquivo JSON de estatísticas usando o utilitário `json_to_markdown.py`.

### 📊 Dados Coletados
- Estatísticas de download em tempo real
- Métricas de processamento por arquivo e tipo
- Informações detalhadas do banco de dados
- Tempos de execução precisos

### 🎯 Finalidade
- Análise de performance do processamento
- Identificação de gargalos
- Documentação de execuções
- Auditoria e relatórios

---

*Gerado em: {current_time}*
"""

    return markdown

def convert_json_to_markdown(json_file_path: str) -> str:
    """Converte arquivo JSON de estatísticas para Markdown"""
    
    # Verificar se o arquivo existe
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Arquivo JSON não encontrado: {json_file_path}")
    
    # Ler dados do JSON
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Erro ao decodificar JSON: {e}")
    except Exception as e:
        raise Exception(f"Erro ao ler arquivo JSON: {e}")
    
    # Gerar Markdown
    markdown_content = generate_markdown_report(json_data)
    
    # Determinar nome do arquivo Markdown (mesmo nome do JSON, mas com extensão .md)
    base_name = os.path.splitext(json_file_path)[0]
    markdown_file_path = f"{base_name}.md"
    
    # Salvar arquivo Markdown
    try:
        with open(markdown_file_path, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
    except Exception as e:
        raise Exception(f"Erro ao salvar arquivo Markdown: {e}")
    
    return markdown_file_path

def main():
    """Função principal para uso via linha de comando"""
    if len(sys.argv) != 2:
        print("Uso: python json_to_markdown.py <arquivo_json>")
        print("Exemplo: python json_to_markdown.py logs/estatisticas_cnpj_20250527_103846.json")
        sys.exit(1)
    
    json_file = sys.argv[1]
    
    try:
        markdown_file = convert_json_to_markdown(json_file)
        print(f"✅ Relatório Markdown gerado com sucesso!")
        print(f"📄 Arquivo: {markdown_file}")
    except Exception as e:
        print(f"❌ Erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 