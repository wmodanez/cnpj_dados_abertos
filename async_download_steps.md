# Plano para Implementar Downloads Assíncronos com Asyncio e Aiohttp

A tabela abaixo detalha as etapas sugeridas e prompts que podem ser usados para implementar a otimização de downloads paralelos com `asyncio` e `aiohttp`.

| Etapa | Descrição                                         | Prompt Sugerido                                                                                                                               |
|-------|---------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| 1     | Instalação de Dependências                        | `Como instalar aiohttp e dependências para downloads assíncronos? (Ex: requirements.txt / pip)`                                                 |
| 2     | Análise do Código Existente                       | `Como analisar o código atual de download para identificar pontos de modificação para asyncio?`                                                 |
| 3     | Estruturação do Módulo de Downloads Assíncronos   | `Crie um esqueleto de módulo para download assíncrono com asyncio e aiohttp (incluindo funções para obter URLs, baixar e monitorar).`             |
| 4     | Implementação da Função de Download Assíncrono    | `Crie uma função assíncrona completa para download com aiohttp (incluindo timeout, retry, progresso e verificação de integridade).`            |
| 5     | Controlador de Concorrência (Semáforo)            | `Como implementar um semáforo asyncio para limitar downloads simultâneos?`                                                                    |
| 6     | Integração com Sistema de Cache                   | `Como integrar o novo sistema de download assíncrono com o cache existente (verificar antes de baixar)?`                                        |
| 7     | Implementação de Barra de Progresso               | `Exemplo de implementação de barra de progresso em tempo real para múltiplos downloads assíncronos (ex: tqdm).`                               |
| 8     | Tratamento Robusto de Erros                       | `Como implementar tratamento robusto de erros para downloads assíncronos (conexão, timeout, retry)?`                                            |
| 9     | Verificação de Integridade dos Arquivos           | `Crie uma função assíncrona para verificar integridade de arquivos baixados (MD5/SHA-256).`                                                    |
| 10    | Testes de Performance                             | `Como criar testes para comparar performance do download assíncrono vs. sequencial (tempo, recursos, confiabilidade)?`                          |
| 11    | Integração com o Código Principal                 | `Como integrar o novo módulo de download assíncrono ao fluxo principal sem quebrar o existente?`                                                |
| 12    | Documentação                                      | `Ajude a documentar a implementação de downloads assíncronos (funcionamento, benefícios, uso).`                                                | 