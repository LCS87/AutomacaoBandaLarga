# AutomacaoBandaLarga — Pipeline de Prospecção

Pipeline modular em Python para processar, enriquecer e segmentar dados de prospecção de clientes para banda larga. Cada etapa consome a saída da anterior e pode ser executada individualmente ou em sequência via orquestrador.

---

## Fluxo do Pipeline

```
CSV bruto
   │
   ▼
[Etapa 1] Separação e Classificação de Prospects
   │  Saída: Excel MEI / Demais clientes por UF
   ▼
[Etapa 2] Enriquecimento de CNPJ via API
   │  Saída: Excel com dados cadastrais completos
   ▼
[Etapa 3] Consulta de Operadora (opcional — adiciona coluna)
   │  Saída: Excel com coluna Operadora
   ▼
[Etapa 4] Separação por Operadora para Discadora
      Saída: CSVs separados por operadora
```

---

## Estrutura do Projeto

```
.
├── pipeline_runner.py          # Orquestrador — executa todas as etapas
├── pyproject.toml              # Dependências, ruff, mypy, pytest
├── .env.example                # Template de variáveis de ambiente
├── .pre-commit-config.yaml     # Hooks de qualidade (ruff + mypy)
│
├── scripts/
│   ├── etapa_1/
│   │   ├── separador.py        # Classifica prospects em MEI / Demais
│   │   └── dados_prospect/     # [entrada] CSVs brutos aqui
│   ├── etapa_2/
│   │   ├── ConsultaCnpj.py     # Enriquece CNPJs via API externa
│   │   ├── Consultar/          # [entrada] Excel da etapa 1 aqui
│   │   └── Consultado/         # [saída]  Excel enriquecido
│   ├── etapa_3/
│   │   ├── consulta_operadora.py
│   │   ├── arquivos_de_entrada/
│   │   └── arquivos_com_operadora/
│   └── etapa_4/
│       ├── separador_operadora.py
│       ├── entrada/            # [entrada] Excel da etapa 2 aqui
│       └── saida/              # [saída]  CSVs por operadora
│
├── config/
│   └── classificacao.py        # Regras de elegibilidade (frozensets)
├── domain/
│   └── empresa.py              # Dataclass Empresa (modelo de domínio)
├── infra/
│   ├── cnpj_api_client.py      # Cliente HTTP da API de CNPJ
│   └── cnpj_cache.py           # Cache persistente com TTL
├── utils/
│   ├── audit_logger.py         # Log de auditoria estruturado (JSON Lines)
│   ├── rate_limiter.py         # Rate limiter sliding window thread-safe
│   └── telefone.py             # Verificação de operadora via phonenumbers
└── tests/
    ├── test_classificacao.py
    ├── test_cnpj_cache.py
    ├── test_cache_expirado.py
    ├── test_empresa.py
    ├── test_format_cnpj.py
    └── test_verificar_operadora.py
```

---

## Tecnologias e Bibliotecas

| Biblioteca | Versão mínima | Uso |
|---|---|---|
| `pandas` | 2.0 | Leitura/escrita de CSV e Excel, transformações de dados |
| `openpyxl` | 3.1 | Engine de leitura/escrita de `.xlsx` |
| `xlsxwriter` | 3.1 | Engine alternativo para escrita de `.xlsx` com abas |
| `requests` | 2.31 | Requisições HTTP para a API de CNPJ |
| `tqdm` | 4.65 | Barra de progresso nas consultas em lote |
| `phonenumbers` | 8.13 | Parsing e identificação de operadora de telefones BR |
| `python-dotenv` | 1.0 | Carregamento de variáveis de ambiente do `.env` |
| `pytest` | 8.0 | Framework de testes unitários |
| `pytest-cov` | 5.0 | Relatório de cobertura de testes |
| `ruff` | 0.4 | Linter e formatter (substitui flake8 + isort + black) |
| `mypy` | 1.10 | Verificação estática de tipos (`--strict`) |
| `pre-commit` | 3.7 | Hooks automáticos de qualidade no commit |

**Python requerido:** 3.11+

---

## Configuração Inicial

### 1. Clonar e instalar dependências

```bash
# Instalar dependências de produção
pip install -e .

# Instalar dependências de desenvolvimento (testes, linting)
pip install -e ".[dev]"
```

### 2. Configurar variáveis de ambiente

```bash
# Windows
copy .env.example .env

# Linux/macOS
cp .env.example .env
```

Editar o `.env` com os valores reais:

```env
# Obrigatório para a Etapa 2
CNPJ_API_KEY=sua_chave_aqui
CNPJ_API_URL=https://api.seuservico.com/v1/cnpj

# Opcionais (já têm padrão)
MAX_REQUESTS_PER_MINUTE=900
REQUEST_TIMEOUT=10
MAX_WORKERS=10
CACHE_TTL_DAYS=30
CNPJ_COLUMN=DOCUMENTO
LOG_LEVEL=INFO
```

> O `.env` está no `.gitignore`. Nunca commite credenciais reais.

### 3. Ativar hooks de qualidade (opcional)

```bash
pre-commit install
```

---

## Execução

### Opção A — Orquestrador (recomendado)

Executa todas as etapas em sequência com relatório consolidado:

```bash
py pipeline_runner.py
```

Executar etapas seletivas:

```bash
py pipeline_runner.py --etapas 1 2
py pipeline_runner.py --etapas 3 4
```

Validar inputs sem processar nada (dry-run):

```bash
py pipeline_runner.py --dry-run
```

Interromper na primeira falha:

```bash
py pipeline_runner.py --fail-fast
```

---

### Opção B — Etapas individuais

#### Etapa 1 — Separação e Classificação de Prospects

Coloque os CSVs brutos em `scripts/etapa_1/dados_prospect/` e execute:

```bash
py scripts/etapa_1/separador.py
```

Saída: `Prospect XX - MEI.xlsx` e `Prospect XX - Demais clientes.xlsx` na mesma pasta.

Mova os Excel gerados para `scripts/etapa_2/Consultar/`.

---

#### Etapa 2 — Enriquecimento de CNPJ

Requer `CNPJ_API_KEY` e `CNPJ_API_URL` configurados no `.env`.

```bash
py scripts/etapa_2/ConsultaCnpj.py
```

Saída: Excel enriquecido em `scripts/etapa_2/Consultado/`.

- Cache com TTL de 30 dias em `Dados do cokpit/cnpj_cache.json`
- CNPJs com falha salvos em `*_falhas_*.txt`
- Consultas paralelas com rate limiting automático

Mova os Excel para `scripts/etapa_4/entrada/`.

---

#### Etapa 3 — Consulta de Operadora (opcional)

Adiciona coluna `Operadora` sem separar por arquivo:

```bash
py scripts/etapa_3/consulta_operadora.py
```

Entrada: `scripts/etapa_3/arquivos_de_entrada/`
Saída: `scripts/etapa_3/arquivos_com_operadora/`

---

#### Etapa 4 — Separação por Operadora para Discadora

```bash
py scripts/etapa_4/separador_operadora.py
```

Entrada: `scripts/etapa_4/entrada/`
Saída: CSVs separados por operadora em `scripts/etapa_4/saida/`

Exemplo de saída: `Prospect_BA__VIVO.csv`, `Prospect_BA__CLARO.csv`, `Prospect_BA__LINHA_FIXA.csv`

---

### Flag --dry-run (todos os scripts)

Todos os scripts aceitam `--dry-run`: lê e valida os arquivos de entrada, loga o que seria processado, mas não gera nenhum arquivo de saída nem consulta APIs.

```bash
py scripts/etapa_1/separador.py --dry-run
py scripts/etapa_2/ConsultaCnpj.py --dry-run
py scripts/etapa_3/consulta_operadora.py --dry-run
py scripts/etapa_4/separador_operadora.py --dry-run
```

---

## Testes

```bash
# Rodar todos os testes
py -m pytest

# Com relatório de cobertura
py -m pytest --cov=. --cov-report=term-missing

# Rodar um módulo específico
py -m pytest tests/test_cnpj_cache.py -v
```

86 testes unitários cobrindo as funções críticas do pipeline. Todos rodam offline em menos de 1 segundo, sem dependências externas ou mocks.

| Módulo de teste | O que cobre |
|---|---|
| `test_classificacao.py` | Regras MEI/DEMAIS/DESCARTAR, frozensets, tipos inválidos |
| `test_format_cnpj.py` | Formatação e validação de CNPJ (14 dígitos) |
| `test_verificar_operadora.py` | Parsing de telefone BR, correção do 9º dígito, retornos |
| `test_cnpj_cache.py` | get/set, dirty flag, flush, load, persistência JSON |
| `test_cache_expirado.py` | TTL parametrizado (7 combinações), expiração no limite |
| `test_empresa.py` | Dataclass Empresa, to_dict, ausência de campos de infra |

---

## Qualidade de Código

```bash
# Lint e auto-fix
py -m ruff check . --fix

# Formatação
py -m ruff format .

# Verificação de tipos
py -m mypy utils/ config/ domain/ infra/ --strict
```

---

## Variáveis de Ambiente

| Variável | Padrão | Obrigatório | Descrição |
|---|---|---|---|
| `CNPJ_API_KEY` | — | Sim (etapa 2) | Chave de autenticação da API de CNPJ |
| `CNPJ_API_URL` | — | Sim (etapa 2) | URL base da API (ex: `https://api.exemplo.com/v1/cnpj`) |
| `MAX_REQUESTS_PER_MINUTE` | `900` | Não | Limite de requisições por minuto (rate limiter) |
| `REQUEST_TIMEOUT` | `10` | Não | Timeout em segundos por requisição HTTP |
| `MAX_WORKERS` | `10` | Não | Threads paralelas para consulta de CNPJ |
| `CACHE_TTL_DAYS` | `30` | Não | Dias até expirar uma entrada do cache de CNPJ |
| `CNPJ_COLUMN` | `DOCUMENTO` | Não | Nome da coluna de CNPJ nos arquivos de entrada |
| `LOG_LEVEL` | `INFO` | Não | Nível de log: `DEBUG`, `INFO`, `WARNING`, `ERROR` |

---

## Auditoria

Cada execução grava eventos estruturados em `audit.jsonl` na raiz do projeto (formato JSON Lines). O arquivo registra início, conclusão e erros de cada etapa com campos: `timestamp`, `script`, `evento`, `mensagem`, `usuario`, `arquivo`, `total`, `processados`, `falhas`, `duracao_s`.

Exemplo de entrada:

```json
{"timestamp": "2026-03-23T14:00:01.123Z", "script": "2-consulta-cnpj", "evento": "conclusao", "mensagem": "Execução concluída", "arquivo": "Prospect_SP.xlsx", "total": 1500, "processados": 1487, "falhas": 13, "duracao_s": 42.7, "usuario": "operador"}
```

---

## Arquitetura

O projeto segue separação de camadas inspirada em DDD:

- `config/` — regras de negócio imutáveis (frozensets de classificação)
- `domain/` — modelos de domínio puros sem dependências de infraestrutura
- `infra/` — implementações de I/O (HTTP, cache em disco)
- `utils/` — utilitários compartilhados (rate limiter, telefone, auditoria)
- `scripts/` — orquestração de cada etapa do pipeline (entrada/saída de arquivos)
