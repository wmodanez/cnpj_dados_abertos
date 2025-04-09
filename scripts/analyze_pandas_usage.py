import ast
import os
from pathlib import Path
from typing import Dict, List, Set
from datetime import datetime

class PandasAnalyzer(ast.NodeVisitor):
    def __init__(self):
        self.pandas_imports = set()
        self.pandas_usage = set()
        
    def visit_Import(self, node):
        for alias in node.names:
            if alias.name == 'pandas':
                self.pandas_imports.add(f"import {alias.name}")
        self.generic_visit(node)
        
    def visit_ImportFrom(self, node):
        if node.module == 'pandas':
            for alias in node.names:
                self.pandas_imports.add(f"from pandas import {alias.name}")
        self.generic_visit(node)
        
    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name):
                if node.func.value.id in ['pd', 'pandas']:
                    self.pandas_usage.add(f"{node.func.value.id}.{node.func.attr}")
        self.generic_visit(node)

def analyze_file(file_path: Path) -> Dict[str, Set[str]]:
    with open(file_path, 'r', encoding='utf-8') as f:
        tree = ast.parse(f.read())
    
    analyzer = PandasAnalyzer()
    analyzer.visit(tree)
    
    return {
        'imports': analyzer.pandas_imports,
        'usage': analyzer.pandas_usage
    }

def scan_project(project_root: Path) -> List[Dict]:
    results = []
    
    for root, _, files in os.walk(project_root):
        for file in files:
            if file.endswith('.py'):
                file_path = Path(root) / file
                try:
                    analysis = analyze_file(file_path)
                    if analysis['imports'] or analysis['usage']:
                        results.append({
                            'file': str(file_path.relative_to(project_root)),
                            **analysis
                        })
                except Exception as e:
                    print(f"Error analyzing {file_path}: {e}")
    
    return results

def generate_report(results: List[Dict], output_path: Path) -> None:
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("# Inventário de Uso do Pandas\n\n")
        f.write(f"Data da análise: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Sumário
        total_files = len(results)
        total_imports = sum(len(r['imports']) for r in results)
        total_usage = sum(len(r['usage']) for r in results)
        
        f.write("## Sumário\n")
        f.write(f"- Total de arquivos com Pandas: {total_files}\n")
        f.write(f"- Total de importações: {total_imports}\n")
        f.write(f"- Total de usos: {total_usage}\n\n")
        
        # Detalhes por arquivo
        f.write("## Detalhes por Arquivo\n\n")
        for result in sorted(results, key=lambda x: x['file']):
            f.write(f"### {result['file']}\n\n")
            
            if result['imports']:
                f.write("#### Importações:\n")
                for imp in sorted(result['imports']):
                    f.write(f"- {imp}\n")
                f.write("\n")
            
            if result['usage']:
                f.write("#### Usos:\n")
                for use in sorted(result['usage']):
                    f.write(f"- {use}\n")
                f.write("\n")
            
            f.write("---\n\n")

if __name__ == '__main__':
    project_root = Path(__file__).parent.parent
    results = scan_project(project_root)
    
    # Gerar relatório em arquivo
    output_file = project_root / "docs" / "pandas_inventory.txt"
    output_file.parent.mkdir(exist_ok=True)
    generate_report(results, output_file)
    
    print(f"\nRelatório gerado em: {output_file}")
    print(f"Total de arquivos analisados: {len(results)}")
