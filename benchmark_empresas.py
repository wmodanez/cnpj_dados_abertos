            # 1. Gráfico comparativo das principais métricas
            fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(16, 12))
            fig.suptitle('Comparação de Performance - Processamento de Empresas', fontsize=16)
            
            # Tempos
            tempo_data = {m: self.resultados[m]['tempo_total'] for m in metodos_validos}
            ax1 = axes[0, 0]
            bars1 = ax1.bar(tempo_data.keys(), tempo_data.values(), color=['steelblue', 'forestgreen', 'firebrick'])
            ax1.set_title('Tempo Total de Execução (s)')
            ax1.set_ylabel('Segundos')
            for bar in bars1:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{height:.1f}s', ha='center', va='bottom')
            
            # Memória
            memoria_data = {m: self.resultados[m]['memoria_pico'] for m in metodos_validos}
            ax2 = axes[0, 1]
            bars2 = ax2.bar(memoria_data.keys(), memoria_data.values(), color=['steelblue', 'forestgreen', 'firebrick'])
            ax2.set_title('Uso de Memória (Pico, %)')
            ax2.set_ylabel('Percentual de Uso (%)')
            for bar in bars2:
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{height:.1f}%', ha='center', va='bottom')
            
            # CPU
            cpu_data = {m: self.resultados[m]['cpu_medio'] for m in metodos_validos}
            ax3 = axes[1, 0]
            bars3 = ax3.bar(cpu_data.keys(), cpu_data.values(), color=['steelblue', 'forestgreen', 'firebrick'])
            ax3.set_title('Uso de CPU (Média, %)')
            ax3.set_ylabel('Percentual de Uso (%)')
            for bar in bars3:
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{height:.1f}%', ha='center', va='bottom')
            
            # Espaço em disco
            disco_data = {m: self.resultados[m]['espaco_disco'] for m in metodos_validos}
            ax4 = axes[1, 1]
            bars4 = ax4.bar(disco_data.keys(), disco_data.values(), color=['steelblue', 'forestgreen', 'firebrick'])
            ax4.set_title('Espaço em Disco (MB)')
            ax4.set_ylabel('MB')
            for bar in bars4:
                height = bar.get_height()
                ax4.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{height:.1f} MB', ha='center', va='bottom')
            
            plt.tight_layout(rect=[0, 0, 1, 0.96])
            plt.savefig(os.path.join(docs_dir, 'benchmark_comparacao.png'), dpi=100)
            plt.close()
            results['comparacao'] = True
            
            # 2. Gráficos detalhados de tempo para cada método
            for metodo in metodos_validos:
                fig, ax = plt.subplots(figsize=(10, 6))
                tempo_total = self.resultados[metodo]['tempo_total']
                extracao = self.resultados[metodo]['tempo_extracao']
                processamento = self.resultados[metodo]['tempo_processamento']
                outros = tempo_total - extracao - processamento
                
                tempos = [extracao, processamento, outros]
                labels = ['Extração', 'Processamento', 'Outros']
                cores = ['#ff9999', '#66b3ff', '#99ff99']
                
                wedges, texts, autotexts = ax.pie(
                    tempos, 
                    labels=labels, 
                    colors=cores,
                    autopct='%1.1f%%', 
                    startangle=90,
                    textprops={'fontsize': 12}
                )
                
                # Adicionar valores absolutos
                for i, texto in enumerate(autotexts):
                    texto.set_text(f'{tempos[i]:.1f}s ({texto.get_text()})')
                
                ax.set_title(f'Distribuição de Tempo - {metodo.upper()}')
                plt.savefig(os.path.join(docs_dir, f'benchmark_tempo_{metodo}.png'), dpi=100)
                plt.close()
                results[f'tempo_{metodo}'] = True
                
            return results
        except Exception as e:
            logger.error(f"Erro ao gerar gráficos: {str(e)}")
            logger.debug(traceback.format_exc())
            return {}
    
    def imprimir_relatorio(self):
        """Imprime um relatório resumido dos resultados."""
        metodos_validos = [m for m, v in self.resultados.items() if v['tempo_total'] > 0]
        if len(metodos_validos) < 2:
            logger.warning("Não há dados suficientes para gerar relatório comparativo")
            return
            
        print("\n" + "="*60)
        print(" "*20 + "RELATÓRIO COMPARATIVO")
        print("="*60)
        
        comparacao = self.comparar_resultados()
        melhor_metodo = comparacao['melhor_metodo'].upper()
        
        print(f"\nMelhor método geral: {melhor_metodo}")
        print(f"Vantagens por critério:")
        
        for criterio, resultado in comparacao['comparacao'].items():
            melhor = resultado['melhor'].upper()
            diferenca = f"{resultado['diferenca_percentual']:.1f}%"
            criterio_formatado = criterio.replace('_', ' ').title()
            print(f"- {criterio_formatado}: {melhor} (diferença: {diferenca})")
        
        print("\nResultados detalhados:")
        for metodo in metodos_validos:
            print(f"\n{metodo.upper()}:")
            print(f"- Tempo total: {self.formatar_tempo(self.resultados[metodo]['tempo_total'])}")
            print(f"- Memória (pico): {self.resultados[metodo]['memoria_pico']:.1f}%")
            print(f"- CPU (média): {self.resultados[metodo]['cpu_medio']:.1f}%")
            print(f"- Espaço em disco: {self.resultados[metodo]['espaco_disco']:.1f} MB")
            print(f"- Taxa de compressão: {self.resultados[metodo]['compressao_taxa']:.1f}%")
        
        print("="*60)
    
    def gerar_relatorio_markdown(self, timestamp):
        """Gera um relatório em formato Markdown para documentação."""
        docs_dir = os.path.join(self.path_base, 'docs')
        os.makedirs(docs_dir, exist_ok=True)
        
        md_path = os.path.join(docs_dir, f'benchmark_empresas_{timestamp}.md')
        
        # Gerar o relatório em Markdown
        relatorio = f"# Benchmark de Processamento de Empresas\n\n"
        relatorio += f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n"
        
        # Adicionar informações do sistema
        relatorio += "## Informações do Sistema\n\n"
        info_sistema = InfoSistema.coletar_informacoes()
        relatorio += f"- **Sistema Operacional:** {info_sistema['sistema']} {info_sistema['versao_sistema']}\n"
        relatorio += f"- **Processador:** {info_sistema['processador']}\n"
        relatorio += f"- **Núcleos:** {info_sistema['cores_fisicos']} físicos, {info_sistema['cores_logicos']} lógicos\n"
        relatorio += f"- **Memória Total:** {info_sistema['memoria_total']}\n"
        
        if GPUTIL_AVAILABLE and 'gpu' in info_sistema and info_sistema['gpu'] != "Nenhuma GPU detectada":
            relatorio += f"- **GPU:** {info_sistema['gpu']}\n"
            relatorio += f"- **Memória GPU:** {info_sistema['memoria_gpu']}\n"
        
        # Adicionar informações dos arquivos
        relatorio += "\n## Informações dos Arquivos\n\n"
        relatorio += "| Arquivo | Tamanho (MB) | Arquivos CSV |\n"
        relatorio += "|---------|--------------|-------------|\n"
        
        for zip_file, info in self.info_arquivo.items():
            relatorio += f"| {zip_file} | {info['tamanho_mb']:.2f} | {info.get('num_csvs', 'N/A')} |\n"
        
        # Adicionar resultados do Pandas
        relatorio += "\n## Resultados\n\n"
        relatorio += "### Resultados Pandas\n\n"
        relatorio += f"- **Tempo Total:** {self.formatar_tempo(self.resultados['pandas']['tempo_total'])}\n"
        relatorio += f"- **Tempo de Extração:** {self.formatar_tempo(self.resultados['pandas']['tempo_extracao'])}\n"
        relatorio += f"- **Tempo de Processamento:** {self.formatar_tempo(self.resultados['pandas']['tempo_processamento'])}\n"
        relatorio += f"- **Memória (pico):** {self.resultados['pandas']['memoria_pico']:.2f}%\n"
        relatorio += f"- **Memória (média):** {self.resultados['pandas']['memoria_media']:.2f}%\n"
        relatorio += f"- **CPU (pico):** {self.resultados['pandas']['cpu_pico']:.2f}%\n"
        relatorio += f"- **CPU (média):** {self.resultados['pandas']['cpu_medio']:.2f}%\n"
        relatorio += f"- **Espaço em Disco:** {self.resultados['pandas']['espaco_disco']:.2f} MB\n"
        relatorio += f"- **Taxa de Compressão:** {self.resultados['pandas']['compressao_taxa']:.2f}%\n\n"
        
        # Adicionar resultados do Dask
        relatorio += "### Resultados Dask\n\n"
        relatorio += f"- **Tempo Total:** {self.formatar_tempo(self.resultados['dask']['tempo_total'])}\n"
        relatorio += f"- **Tempo de Extração:** {self.formatar_tempo(self.resultados['dask']['tempo_extracao'])}\n"
        relatorio += f"- **Tempo de Processamento:** {self.formatar_tempo(self.resultados['dask']['tempo_processamento'])}\n"
        relatorio += f"- **Memória (pico):** {self.resultados['dask']['memoria_pico']:.2f}%\n"
        relatorio += f"- **Memória (média):** {self.resultados['dask']['memoria_media']:.2f}%\n"
        relatorio += f"- **CPU (pico):** {self.resultados['dask']['cpu_pico']:.2f}%\n"
        relatorio += f"- **CPU (média):** {self.resultados['dask']['cpu_medio']:.2f}%\n"
        relatorio += f"- **Espaço em Disco:** {self.resultados['dask']['espaco_disco']:.2f} MB\n"
        relatorio += f"- **Taxa de Compressão:** {self.resultados['dask']['compressao_taxa']:.2f}%\n\n"
        
        # Adicionar resultados do Polars
        relatorio += "### Resultados Polars\n\n"
        relatorio += f"- **Tempo Total:** {self.formatar_tempo(self.resultados['polars']['tempo_total'])}\n"
        relatorio += f"- **Tempo de Extração:** {self.formatar_tempo(self.resultados['polars']['tempo_extracao'])}\n"
        relatorio += f"- **Tempo de Processamento:** {self.formatar_tempo(self.resultados['polars']['tempo_processamento'])}\n"
        relatorio += f"- **Memória (pico):** {self.resultados['polars']['memoria_pico']:.2f}%\n"
        relatorio += f"- **Memória (média):** {self.resultados['polars']['memoria_media']:.2f}%\n"
        relatorio += f"- **CPU (pico):** {self.resultados['polars']['cpu_pico']:.2f}%\n"
        relatorio += f"- **CPU (média):** {self.resultados['polars']['cpu_medio']:.2f}%\n"
        relatorio += f"- **Espaço em Disco:** {self.resultados['polars']['espaco_disco']:.2f} MB\n"
        relatorio += f"- **Taxa de Compressão:** {self.resultados['polars']['compressao_taxa']:.2f}%\n\n"
        
        # Comparação dos resultados
        comparacao = self.comparar_resultados()
        relatorio += "### Comparação\n\n"
        
        relatorio += "| Métrica | Melhor Método | Diferença |\n"
        relatorio += "|---------|---------------|----------|\n"
        
        for criterio, resultado in comparacao['comparacao'].items():
            melhor = resultado['melhor'].upper()
            diferenca = f"{resultado['diferenca_percentual']:.2f}%"
            metrica_formatada = criterio.replace('_', ' ').title()
            relatorio += f"| {metrica_formatada} | **{melhor}** | {diferenca} |\n"
        
        relatorio += "\n"
        relatorio += f"**Conclusão:** {comparacao['melhor_metodo'].upper()} é o método mais adequado, "
        relatorio += f"vencendo em {comparacao['contagem'][comparacao['melhor_metodo']]} de {len(comparacao['comparacao'])} critérios.\n\n"
        
        # Verificar se os gráficos existem antes de incluí-los no relatório
        grafico_comparacao_path = os.path.join(docs_dir, 'benchmark_comparacao.png')
        if os.path.exists(grafico_comparacao_path):
            relatorio += "## Gráficos\n\n"
            relatorio += "### Gráfico de Comparação\n\n"
            relatorio += "![Gráfico de Comparação](benchmark_comparacao.png)\n\n"
            
            # Adicionar gráficos de tempo se existirem
            grafico_tempo_pandas = os.path.join(docs_dir, 'benchmark_tempo_pandas.png')
            if os.path.exists(grafico_tempo_pandas):
                relatorio += "### Distribuição de Tempo - Pandas\n\n"
                relatorio += "![Tempo Pandas](benchmark_tempo_pandas.png)\n\n"
                
            grafico_tempo_dask = os.path.join(docs_dir, 'benchmark_tempo_dask.png')
            if os.path.exists(grafico_tempo_dask):
                relatorio += "### Distribuição de Tempo - Dask\n\n"
                relatorio += "![Tempo Dask](benchmark_tempo_dask.png)\n\n"
            
            grafico_tempo_polars = os.path.join(docs_dir, 'benchmark_tempo_polars.png')
            if os.path.exists(grafico_tempo_polars):
                relatorio += "### Distribuição de Tempo - Polars\n\n"
                relatorio += "![Tempo Polars](benchmark_tempo_polars.png)\n\n"
        else:
            relatorio += "## Gráficos\n\n"
            relatorio += "*Não foi possível gerar gráficos para este relatório.*\n\n"
        
        # Salvar o relatório
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(relatorio)
        
        logger.info(f"Relatório completo gerado: {md_path}")
        return md_path


def main():
    """Função principal."""
    # Adicionar timestamp no início da execução
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Configuração do parser de argumentos
    parser = argparse.ArgumentParser(description='Benchmark para comparar processamento de Empresas com Pandas, Dask e Polars')
    parser.add_argument('--path_zip', type=str, default='dados-abertos-zip', 
                        help='Caminho para o diretório com os arquivos ZIP')
    parser.add_argument('--arquivo_zip', type=str, 
                        help='Arquivo ZIP específico para análise')
    parser.add_argument('--path_base', type=str, default='benchmark_temp', 
                        help='Caminho base para criar diretórios temporários')
    parser.add_argument('--parquet_destino', type=str, default='parquet',
                        help='Caminho para os arquivos parquet gerados')
    parser.add_argument('--limpar', action='store_true', 
                        help='Limpar diretórios temporários')
    parser.add_argument('--pandas', action='store_true', 
                        help='Executar apenas o benchmark com Pandas')
    parser.add_argument('--dask', action='store_true', 
                        help='Executar apenas o benchmark com Dask')
    parser.add_argument('--polars', action='store_true',
                        help='Executar apenas o benchmark com Polars')
    parser.add_argument('--graficos', action='store_true', 
                        help='Gerar gráficos comparativos')
    parser.add_argument('--completo', action='store_true',
                        help='Executar todos os benchmarks e gerar gráficos')
    
    # Adicionar argumentos para controle de log
    add_log_level_argument(parser)
    
    args = parser.parse_args()
    
    # Criar diretórios necessários
    os.makedirs(args.path_base, exist_ok=True)
    logs_dir = os.path.join(args.path_base, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    docs_dir = os.path.join(args.path_base, "docs")
    os.makedirs(docs_dir, exist_ok=True)
    
    # Configurar logging
    log_file = os.path.join(logs_dir, f"benchmark_{timestamp}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    configure_logging(args)
    
    logger.info(f"Iniciando benchmark. Log: {log_file}")
    
    # Verificações básicas
    if not os.path.exists(args.path_zip):
        print(f"Diretório {args.path_zip} não encontrado.")
        return
    
    if args.arquivo_zip:
        arquivo_zip_path = os.path.join(args.path_zip, args.arquivo_zip)
        if not os.path.exists(arquivo_zip_path):
            print(f"Arquivo ZIP {arquivo_zip_path} não encontrado.")
            return
    
    # Informações do sistema
    try:
        info_sistema = InfoSistema.coletar_informacoes()
        InfoSistema.imprimir_informacoes(info_sistema)
    except Exception as e:
        logger.error(f"Erro ao coletar informações do sistema: {str(e)}")
    
    # Flags para controlar quais métodos foram executados
    pandas_executado = False
    dask_executado = False
    polars_executado = False
    
    try:
        # Inicializar o benchmark
        benchmark = BenchmarkEmpresas(
            path_zip=args.path_zip, 
            path_base=args.path_base, 
            arquivo_zip_especifico=args.arquivo_zip,
            path_parquet_destino=args.parquet_destino,
            executar_limpeza=args.limpar
        )
        
        # Limpar diretórios docs e logs no início, se solicitado
        if args.limpar:
            print("\nLimpando diretórios antes de iniciar o benchmark...")
            benchmark.limpar_diretorios(preservar_parquet=False, limpar_docs_logs=True)
        
        # Executar os benchmarks
        try:
            # Verificar que combinação de flags usar
            executar_todos = args.completo
            executar_especificos = args.pandas or args.dask or args.polars
            
            # Se nenhuma flag específica foi fornecida e não é completo, executar pandas por padrão
            if not executar_especificos and not executar_todos:
                args.pandas = True
            
            # Executa Pandas se solicitado ou se completo e não específico para outros
            if args.pandas or (executar_todos and not (args.dask or args.polars)):
                benchmark.executar_benchmark_pandas()
                pandas_executado = True
                
            # Executa Dask se solicitado ou se completo e não específico para outros
            if args.dask or (executar_todos and not (args.pandas or args.polars)):
                benchmark.executar_benchmark_dask()
                dask_executado = True
                
            # Executa Polars se solicitado ou se completo e não específico para outros  
            if args.polars or (executar_todos and not (args.pandas or args.dask)):
                benchmark.executar_benchmark_polars()
                polars_executado = True
            
            # Se --completo foi especificado explicitamente, executar todos os métodos não executados ainda
            if args.completo:
                if not pandas_executado:
                    benchmark.executar_benchmark_pandas()
                    pandas_executado = True
                
                if not dask_executado:
                    benchmark.executar_benchmark_dask()
                    dask_executado = True
                
                if not polars_executado:
                    benchmark.executar_benchmark_polars()
                    polars_executado = True
            
            # Gerar gráficos comparativos somente se mais de um método foi executado
            graficos_criados = {}
            metodos_executados = [m for m, flag in [('pandas', pandas_executado), 
                                                   ('dask', dask_executado),
                                                   ('polars', polars_executado)] if flag]
            
            if (args.graficos or args.completo) and len(metodos_executados) >= 2:
                try:
                    graficos_criados = benchmark.gerar_graficos()
                    if not graficos_criados.get('comparacao', False):
                        logger.warning("Não foi possível gerar o gráfico de comparação")
                except Exception as e:
                    logger.error(f"Erro ao gerar gráficos: {str(e)}")
                    logger.debug(traceback.format_exc())
            elif (args.graficos or args.completo) and len(metodos_executados) < 2:
                logger.warning("Gráficos comparativos requerem pelo menos dois métodos")
            
            # Imprimir relatório final
            if len(metodos_executados) >= 2:
                benchmark.imprimir_relatorio()
                
                # Gerar relatório Markdown
                md_path = benchmark.gerar_relatorio_markdown(timestamp)
                logger.info(f"Relatório: {md_path}")
            else:
                # Relatório simplificado para método único
                for metodo in metodos_executados:
                    print("\n" + "="*40)
                    print(" "*15 + metodo.upper())
                    print("="*40)
                    print(f"Tempo: {benchmark.formatar_tempo(benchmark.resultados[metodo]['tempo_total'])}")
                    print(f"Memória: {benchmark.resultados[metodo]['memoria_pico']:.1f}%")
                    print(f"Espaço: {benchmark.resultados[metodo]['espaco_disco']:.1f} MB")
            
            # Limpar diretórios temporários se solicitado
            if args.limpar:
                print("\nLimpando diretórios temporários após benchmark...")
                # Limpar diretórios unzip após o processamento, mas preservar parquet
                benchmark.limpar_diretorios(preservar_parquet=True, limpar_docs_logs=False)
                
        except Exception as e:
            logger.error(f"Erro durante o benchmark: {str(e)}")
            logger.debug(traceback.format_exc())
            
    except Exception as e:
        logger.error(f"Erro ao inicializar benchmark: {str(e)}")
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    main() 