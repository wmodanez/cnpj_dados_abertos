# Sistema de Entidades da Receita Federal

## 📋 Visão Geral

Este módulo contém as entidades implementadas para representar os dados da Receita Federal de forma estruturada, com validações robustas, transformações de dados e sistema de schemas integrado.

## 🏗️ Estrutura Implementada

### Entidades Principais ✅ IMPLEMENTADAS

- **`Empresa.py`** - Entidade para dados de empresas
- **`Estabelecimento.py`** - Entidade para estabelecimentos 
- **`Socio.py`** - Entidade para sócios
- **`Simples.py`** - Entidade para dados do Simples Nacional

### Sistema de Suporte

- **`base.py`** - Classe base e EntityFactory
- **`schemas/`** - Schemas Pydantic para validação
- **`validation/`** - Sistema híbrido de validação

### Entidades Futuras 🚧 PENDENTES

Arquivos vazios reservados para implementação futura:
- `Municipio.py`
- `NaturezaJuridica.py` 
- `QualificacaoSocio.py`
- `SituacaoCadastral.py`
- `Motivo.py`
- `UF.py`
- `Pais.py`
- `Painel.py`
- `Referencia.py`
- `Variavel.py`
- `Lucro.py`

## 🚀 Características Implementadas

### 1. Entidades com Validação Robusta

Cada entidade implementa:
- ✅ Validação de campos obrigatórios
- ✅ Validação de tipos de dados
- ✅ Validação de regras de negócio específicas
- ✅ Transformações automáticas de dados
- ✅ Métodos utilitários específicos

### 2. Sistema de Schemas (Pydantic)

- ✅ Validação declarativa com Pydantic 2.x
- ✅ Conversão automática de tipos
- ✅ Validações customizadas por entidade
- ✅ Documentação automática dos campos
- ✅ Compatibilidade com APIs e serialização

### 3. Sistema de Validação Híbrido

- ✅ Validação em lote para DataFrames grandes
- ✅ Correção automática de dados malformados
- ✅ Relatórios detalhados de validação
- ✅ Fallback para validação customizada quando Pydantic não disponível

### 4. EntityFactory Pattern

- ✅ Criação dinâmica de entidades por tipo
- ✅ Registro automático de entidades
- ✅ Interface unificada para instanciação

## 📊 Entidades Detalhadas

### 🏢 Empresa

**Campos principais:**
- `cnpj_basico` (8 dígitos)
- `razao_social` 
- `natureza_juridica`
- `capital_social`
- `porte_empresa`
- `cpf_extraido` (extraído automaticamente)

**Funcionalidades específicas:**
- ✅ Extração automática de CPF da razão social
- ✅ Validação de CPF contra lista de CPFs inválidos
- ✅ Limpeza da razão social removendo CPF
- ✅ Métodos: `is_microempresa()`, `is_pequena_empresa()`, `is_empresa_privada()`

### 🏪 Estabelecimento

**Campos principais:**
- `cnpj_basico`, `cnpj_ordem`, `cnpj_dv`
- `cnpj_completo` (calculado automaticamente)
- `matriz_filial`
- `nome_fantasia`
- `uf`, `municipio`, `cep`

**Funcionalidades específicas:**
- ✅ Cálculo automático do CNPJ completo
- ✅ Validação completa de CNPJ (algoritmo oficial)
- ✅ Validação de UF brasileiras
- ✅ Métodos: `is_matriz()`, `is_filial()`, `is_ativo()`, `get_cnpj_formatado()`

### 👤 Socio

**Campos principais:**
- `cnpj_basico`
- `nome_socio`
- `cnpj_cpf_socio`
- `data_entrada_sociedade`
- `representante_legal`

**Funcionalidades específicas:**
- ✅ Validação de CPF e CNPJ usando algoritmos oficiais
- ✅ Distinção entre pessoa física e jurídica
- ✅ Métodos: `is_pessoa_fisica()`, `is_pessoa_juridica()`, `get_documento_formatado()`

### 📊 Simples

**Campos principais:**
- `cnpj_basico`
- `opcao_simples`, `data_opcao_simples`, `data_exclusao_simples`
- `opcao_mei`, `data_opcao_mei`, `data_exclusao_mei`

**Funcionalidades específicas:**
- ✅ Validação de datas posteriores a 2006 (criação do Simples)
- ✅ Validação de consistência entre datas
- ✅ Métodos: `is_optante_simples()`, `is_ativo_simples()`, `get_tempo_simples()`

## 🔧 Como Usar

### Importação Básica

```python
from src.Entity import Empresa, Estabelecimento, Socio, Simples
from src.Entity import EntityFactory, EntityValidator
```

### Criar Entidades

```python
# Criar empresa
empresa = Empresa(
    cnpj_basico="12345678",
    razao_social="EMPRESA EXEMPLO LTDA"
)

# Usar Factory
empresa = EntityFactory.create_entity('empresa', dados)
```

### Validação

```python
# Validação individual
validator = EntityValidator('empresa')
resultado = validator.validate_single_row(dados)

# Validação de DataFrame
resultado = validator.validate_dataframe(df)
```

## 📁 Estrutura de Arquivos

```
src/Entity/
├── 📄 __init__.py              # Exports e registros principais
├── 📄 base.py                  # BaseEntity e EntityFactory
├── 📄 Empresa.py              # ✅ Entidade Empresa
├── 📄 Estabelecimento.py      # ✅ Entidade Estabelecimento  
├── 📄 Socio.py                # ✅ Entidade Socio
├── 📄 Simples.py              # ✅ Entidade Simples
├── 📁 schemas/                # Schemas Pydantic específicos
│   ├── empresa.py
│   ├── estabelecimento.py
│   ├── socio.py
│   ├── simples.py
│   └── utils.py
├── 📁 validation/             # Sistema híbrido de validação
│   ├── validator.py
│   ├── batch.py
│   └── corrections.py
└── 📄 [Entidades futuras...]  # Arquivos reservados para implementação
```

## 🧪 Testes

### Executar Testes Básicos

```bash
python tests/test_entities_simple.py
```

### Executar Testes Completos  

```bash
python tests/test_entities.py
```

## 📚 Exemplos

Consulte a pasta `exemplos/` para:
- `exemplo_uso_entidades.py` - Exemplo prático completo
- `exemplos_entidades.py` - Exemplos específicos por entidade

## 🔄 Status da Implementação

### ✅ Concluído
- [x] 4 entidades principais implementadas
- [x] Sistema de validação híbrido (Pydantic + customizado)
- [x] EntityFactory pattern
- [x] Schemas Pydantic 2.x compatíveis
- [x] Transformações automáticas de dados
- [x] Testes básicos e avançados
- [x] Documentação e exemplos

### 🚧 Próximos Passos
- [ ] Implementar entidades restantes conforme necessidade
- [ ] Integração com processadores existentes
- [ ] Performance benchmarks
- [ ] Documentação API completa

## 🎯 Benefícios Alcançados

1. **Validação Robusta**: Dados sempre consistentes e validados
2. **Reutilização**: Entidades usáveis em qualquer contexto
3. **Manutenibilidade**: Lógica centralizada e documentada
4. **Escalabilidade**: Fácil adição de novas entidades
5. **Integração**: Compatível com Pydantic, FastAPI, etc.

---

**Versão:** 1.0.0  
**Compatibilidade:** Python 3.8+, Pydantic 2.x, Polars  
**Status:** ✅ Produção Ready 