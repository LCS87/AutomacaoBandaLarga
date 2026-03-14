# Pipeline de Prospecção — AutomacaoBandaLarga

Pipeline modular em Python para processar, enriquecer e segmentar dados de prospecção. Cada etapa consome a saída da anterior.

---

## Fluxo do Pipeline

```
CSV bruto → [1] Separação → [2] Consulta CNPJ → [3/4] Operadora → CSVs por operadora
```

| Etapa | Script | Entrada | Saída |
|-------|--------|---------|-------|
| 1 | `1 - SeparacaoProspect/separador.py` | CSVs em `dados_prospect/` | Excel MEI / Demais |
| 2 | `2 - ConsultaCnpj/ConsultaCnpj.py` | Excel em `Consultar/` | Excel enriquecido em `Consultado/` |
| 3 | `3 - Consulta_operadora/consulta-operadora.py` | Excel em `arquivos_de_entrada/` | Excel com coluna Operadora |
| 4 | `4 - ArquivoDiscadora/separador_operadora.py` | Excel em `entrada/` | CSVs por operadora em `saida/` |

---

## Configuração

### 1. Instalar dependências

Cada script tem seu próprio `requirements.txt` com apenas o necessário:

```bash
pip install -r "1 - SeparacaoProspect/requirements.txt"
pip install -r "2 - ConsultaCnpj/requirements.txt"
pip install -r "3 - Consulta_operadora/requirements.txt"
pip install -r "4 - ArquivoDiscadora/requirements.txt"
```

Ou instale tudo de uma vez:

```bash
pip install pandas openpyxl xlsxwriter requests tqdm phonenumbers python-dotenv
```

### 2. Configurar variáveis de ambiente (obrigatório para o Script 2)

```bash
# Copie o template e preencha com seus valores
cp .env.example .env
```

Edite o `.env`:
```
CNPJ_API_KEY=sua_chave_aqui
CNPJ_API_URL=https://api.seuservico.com/v1/cnpj
```

> O arquivo `.env` já está no `.gitignore`. Nunca commite credenciais reais.

---

## Execução

### Passo 1 — Separação e Classificação

Coloque os CSVs brutos em `1 - SeparacaoProspect/dados_prospect/` e execute:

```bash
python "1 - SeparacaoProspect/separador.py"
```

Saída: arquivos `Prospect XX - MEI.xlsx` e `Prospect XX - Demais clientes.xlsx` na mesma pasta.

Mova os Excel gerados para `2 - ConsultaCnpj/Consultar/`.

---

### Passo 2 — Enriquecimento de CNPJ

```bash
python "2 - ConsultaCnpj/ConsultaCnpj.py"
```

Saída: Excel enriquecido em `2 - ConsultaCnpj/Consultado/`.

- Usa cache com TTL de 30 dias (configurável via `CACHE_TTL_DAYS` no `.env`)
- CNPJs com falha são salvos em arquivo `_falhas_*.txt`

Mova os Excel enriquecidos para `4 - ArquivoDiscadora/entrada/`.

---

### Passo 3 (opcional) — Consulta de Operadora simples

Se quiser apenas adicionar a coluna de operadora sem separar por arquivo:

```bash
python "3 - Consulta_operadora/consulta-operadora.py"
```

---

### Passo 4 — Separação Final por Operadora

```bash
python "4 - ArquivoDiscadora/separador_operadora.py"
```

Saída: CSVs separados por operadora em `4 - ArquivoDiscadora/saida/`  
Ex: `Prospect_BA__VIVO.csv`, `Prospect_BA__CLARO.csv`, `Prospect_BA__LINHA_FIXA.csv`

---

## Estrutura do Projeto

```
.
├── .env.example                  # Template de configuração
├── utils/
│   ├── telefone.py               # Verificação de operadora (compartilhado entre scripts 3 e 4)
│   └── rate_limiter.py           # Rate limiter thread-safe (usado pelo script 2)
├── 1 - SeparacaoProspect/
├── 2 - ConsultaCnpj/
├── 3 - Consulta_operadora/
└── 4 - ArquivoDiscadora/
```

---

## Variáveis de Ambiente Disponíveis

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `CNPJ_API_KEY` | — | Chave da API de consulta CNPJ (obrigatório) |
| `CNPJ_API_URL` | — | URL base da API (obrigatório) |
| `MAX_REQUESTS_PER_MINUTE` | `900` | Limite de requisições por minuto |
| `REQUEST_TIMEOUT` | `10` | Timeout em segundos por requisição |
| `MAX_WORKERS` | `10` | Threads paralelas para consulta |
| `CACHE_TTL_DAYS` | `30` | Dias até expirar cache de CNPJ |
| `CNPJ_COLUMN` | `DOCUMENTO` | Nome da coluna de CNPJ nos arquivos |
| `LOG_LEVEL` | `INFO` | Nível de log (`DEBUG`, `INFO`, `WARNING`) |
