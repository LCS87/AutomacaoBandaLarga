import os
import sys
import json
import logging
import time
import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm
from dotenv import load_dotenv

# Carrega variáveis do .env (procura na raiz do projeto)
load_dotenv(Path(__file__).parent.parent / ".env")

# ================== CONFIGURAÇÕES ==================
SCRIPT_DIR = Path(__file__).parent

INPUT_FOLDER  = SCRIPT_DIR / "Consultar"
OUTPUT_FOLDER = SCRIPT_DIR / "Consultado"
LOGS_FOLDER   = SCRIPT_DIR / "Dados do cokpit" / "Logs"
CACHE_FILE    = LOGS_FOLDER / "cnpj_cache.json"

API_KEY  = os.getenv("CNPJ_API_KEY", "")
API_URL  = os.getenv("CNPJ_API_URL", "https://api.blablabla.com/v1/cnpj")

MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "900"))
REQUEST_TIMEOUT         = int(os.getenv("REQUEST_TIMEOUT", "10"))
MAX_WORKERS             = int(os.getenv("MAX_WORKERS", "10"))
CACHE_TTL_DAYS          = int(os.getenv("CACHE_TTL_DAYS", "30"))
CNPJ_COLUMN             = os.getenv("CNPJ_COLUMN", "DOCUMENTO")

for folder in (INPUT_FOLDER, OUTPUT_FOLDER, LOGS_FOLDER):
    folder.mkdir(parents=True, exist_ok=True)

# ================== LOGGING ==================
logging.basicConfig(
    filename=str(LOGS_FOLDER / f"cnpj_consultas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================== RATE LIMITER ==================
# Importa o rate limiter correto (sliding window, thread-safe)
# Fallback inline caso o módulo utils não esteja no path
try:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.rate_limiter import RateLimiter
except ImportError:
    import threading
    from collections import deque

    class RateLimiter:
        def __init__(self, max_requests, time_window=60.0):
            self.max_requests = max_requests
            self.time_window = time_window
            self.requests = deque()
            self.lock = threading.Lock()

        def wait_if_needed(self):
            with self.lock:
                now = time.time()
                while self.requests and self.requests[0] < now - self.time_window:
                    self.requests.popleft()
                if len(self.requests) >= self.max_requests:
                    sleep_time = self.time_window - (now - self.requests[0])
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    now = time.time()
                    while self.requests and self.requests[0] < now - self.time_window:
                        self.requests.popleft()
                self.requests.append(time.time())

rate_limiter = RateLimiter(max_requests=MAX_REQUESTS_PER_MINUTE, time_window=60.0)

# ================== CACHE ==================
cnpj_cache: dict = {}
_cache_dirty = False

def _cache_expirado(entry: dict) -> bool:
    """Retorna True se o registro do cache passou do TTL."""
    ts = entry.get("_cached_at")
    if not ts:
        return True
    cached_at = datetime.fromisoformat(ts)
    return datetime.now() - cached_at > timedelta(days=CACHE_TTL_DAYS)

def load_cache():
    global cnpj_cache
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cnpj_cache = json.load(f)
        except Exception as e:
            logger.warning(f"Erro ao carregar cache: {e}. Iniciando novo.")
            cnpj_cache = {}

def save_cache():
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cnpj_cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Erro ao salvar cache: {e}")

# ================== FUNÇÕES BASE ==================
def format_cnpj(cnpj) -> str | None:
    if pd.isna(cnpj):
        return None
    digits = ''.join(filter(str.isdigit, str(cnpj).strip()))
    return digits if len(digits) == 14 else None

def consulta_api(cnpj: str) -> dict | None:
    """Consulta a API de CNPJ com rate limiting e retry básico."""
    if not API_KEY or "blablabla" in API_URL:
        logger.error("API_KEY ou API_URL não configurados. Defina no arquivo .env")
        return None

    url = f"{API_URL.rstrip('/')}/{cnpj}"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    for tentativa in range(3):
        rate_limiter.wait_if_needed()
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                wait = 60 * (tentativa + 1)
                logger.warning(f"Rate limit da API (429). Aguardando {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"Erro {r.status_code} ao consultar {cnpj}: {r.text[:200]}")
                return None
        except requests.Timeout:
            logger.warning(f"Timeout na tentativa {tentativa + 1} para {cnpj}")
        except Exception as e:
            logger.error(f"Erro de conexão ao consultar {cnpj}: {e}")
            return None

    return None

def first_value(data: dict, keys: list):
    for k in keys:
        if v := data.get(k):
            return v
    return ""

def parse_empresa_data(data: dict) -> dict:
    empresa = {
        "CNPJ": first_value(data, ["cnpj"]),
        "RazaoSocial": first_value(data, ["razao_social"]) or "NÃO ENCONTRADO",
        "NomeFantasia": first_value(data, ["nome_fantasia"]),
        "SituacaoCadastral": data.get("situacao", {}).get("nome"),
        "DataAbertura": first_value(data, ["data_inicio"]),
        "NaturezaJuridica": first_value(data, ["natureza_juridica"]),
        "CapitalSocial": first_value(data, ["capital_social"]),
        "Fonte": "API",
        "_cached_at": datetime.now().isoformat()
    }

    endereco = data.get("endereco", {})
    empresa.update({
        "CEP": endereco.get("cep"),
        "Logradouro": endereco.get("logradouro"),
        "Numero": endereco.get("numero"),
        "Complemento": endereco.get("complemento"),
        "Bairro": endereco.get("bairro"),
        "Municipio": endereco.get("municipio"),
        "UF": endereco.get("uf")
    })
    empresa["EnderecoCompleto"] = ", ".join(filter(None, [
        empresa["Logradouro"], empresa["Numero"], empresa["Complemento"],
        empresa["Bairro"], empresa["Municipio"], empresa["UF"], empresa["CEP"]
    ]))

    telefones = []
    for campo in ("telefone1", "telefone2"):
        if t := data.get(campo):
            telefones.append(str(t).strip())
    if t := endereco.get("telefone"):
        telefones.append(str(t).strip())
    empresa["Telefone"] = " / ".join(sorted(set(telefones))) if telefones else ""
    empresa["Email"] = data.get("email") or ""

    if isinstance(data.get("atividade_principal"), dict):
        empresa["AtividadePrincipal"] = data["atividade_principal"].get("descricao", "")
    else:
        empresa["AtividadePrincipal"] = ""

    return empresa

# ================== LÓGICA DE CONSULTA ==================
def get_empresa_data(cnpj: str) -> dict:
    if not cnpj:
        return {"CNPJ": "N/A", "RazaoSocial": "CNPJ INVÁLIDO", "Fonte": "Erro"}

    # Verifica cache (com TTL)
    if cnpj in cnpj_cache:
        entry = cnpj_cache[cnpj]
        if not _cache_expirado(entry) and entry.get('RazaoSocial') != 'NÃO ENCONTRADO':
            return entry

    data = consulta_api(cnpj)
    if data:
        parsed = parse_empresa_data(data)
        cnpj_cache[cnpj] = parsed
        return parsed

    empty = {"CNPJ": cnpj, "RazaoSocial": "NÃO ENCONTRADO", "Fonte": "API",
             "_cached_at": datetime.now().isoformat()}
    cnpj_cache[cnpj] = empty
    return empty

def processar_cnpjs(cnpjs: list) -> list:
    resultados = []
    novos = [c for c in cnpjs if c and (
        c not in cnpj_cache or _cache_expirado(cnpj_cache[c])
    )]

    # Retorna do cache os que já estão válidos
    for c in cnpjs:
        if c in cnpj_cache and not _cache_expirado(cnpj_cache[c]):
            resultados.append(cnpj_cache[c])

    if novos:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(get_empresa_data, c): c for c in novos}
            for i, future in enumerate(tqdm(
                concurrent.futures.as_completed(futures),
                total=len(novos), desc="Consultando CNPJs"
            )):
                cnpj = futures[future]
                try:
                    resultados.append(future.result())
                except Exception as e:
                    logger.error(f"Erro ao processar CNPJ {cnpj}: {e}")
                    resultados.append({"CNPJ": cnpj, "RazaoSocial": "ERRO NA CONSULTA", "Fonte": "Erro"})

                if (i + 1) % 100 == 0:
                    save_cache()

    return resultados

# ================== PROCESSAMENTO DE PLANILHAS ==================
def processar_planilha(arquivo: Path) -> Path | None:
    logger.info(f"Iniciando: {arquivo.name}")

    try:
        if arquivo.suffix == '.xlsx':
            df = pd.read_excel(str(arquivo), dtype={CNPJ_COLUMN: str})
        else:
            df = pd.read_csv(str(arquivo), sep=';', dtype={CNPJ_COLUMN: str})
    except Exception as e:
        logger.error(f"Erro ao ler {arquivo.name}: {e}")
        return None

    if CNPJ_COLUMN not in df.columns:
        logger.error(f"Coluna '{CNPJ_COLUMN}' não encontrada em {arquivo.name}")
        return None

    df['CNPJ_FORMATADO'] = df[CNPJ_COLUMN].apply(format_cnpj)
    cnpjs = [c for c in df['CNPJ_FORMATADO'].unique() if c]

    if not cnpjs:
        logger.error(f"Nenhum CNPJ válido (14 dígitos) em {arquivo.name}")
        return None

    print(f"  CNPJs únicos para consulta: {len(cnpjs)}")

    resultados = processar_cnpjs(cnpjs)
    df_resultado = pd.DataFrame(resultados)

    # Remove coluna interna de cache antes de salvar
    if '_cached_at' in df_resultado.columns:
        df_resultado = df_resultado.drop(columns=['_cached_at'])

    df_final = pd.merge(df, df_resultado, left_on='CNPJ_FORMATADO', right_on='CNPJ', how='left')
    df_final = df_final.drop(columns=['CNPJ_FORMATADO'])

    colunas_ordenadas = [
        'SMARTCODE', 'DOCUMENTO', 'CNPJ', 'RazaoSocial', 'NomeFantasia',
        'SituacaoCadastral', 'DataAbertura', 'NaturezaJuridica', 'CapitalSocial',
        'AtividadePrincipal', 'CEP', 'Logradouro', 'Numero', 'Complemento',
        'Bairro', 'Municipio', 'UF', 'EnderecoCompleto',
        'TELEFONE', 'Telefone', 'Email', 'ENRIQUECIMENTO',
        'FLAGDISPO', 'TIPO_BLOQUEIO_TELEFONE', 'Fonte'
    ]
    df_final = df_final.reindex(columns=[c for c in colunas_ordenadas if c in df_final.columns])

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = OUTPUT_FOLDER / f"{arquivo.stem}_consultado_{ts}.xlsx"
    df_final.to_excel(str(output_file), index=False)

    # Salva CNPJs com falha
    falhas = df_resultado[
        df_resultado['RazaoSocial'].isin(['NÃO ENCONTRADO', 'ERRO NA CONSULTA'])
    ]['CNPJ'].tolist()
    if falhas:
        falhas_file = OUTPUT_FOLDER / f"{arquivo.stem}_falhas_{ts}.txt"
        falhas_file.write_text("\n".join(falhas), encoding='utf-8')
        logger.warning(f"{len(falhas)} CNPJs com falha salvos em {falhas_file.name}")

    logger.info(f"Concluído: {output_file.name}")
    print(f"  Salvo em: {output_file}")
    return output_file

# ================== MAIN ==================
def main():
    if not API_KEY or "blablabla" in API_URL:
        print("\n[AVISO] API_KEY ou API_URL não configurados.")
        print("Crie um arquivo .env na raiz do projeto com CNPJ_API_KEY e CNPJ_API_URL.")
        print("Veja o arquivo .env.example para referência.\n")

    load_cache()

    arquivos = [
        f for f in INPUT_FOLDER.iterdir()
        if f.suffix in ('.xlsx', '.csv') and not f.name.startswith('~')
    ]

    if not arquivos:
        print(f"Nenhum arquivo encontrado em {INPUT_FOLDER}")
        return

    print(f"\nArquivos encontrados: {len(arquivos)}")

    for arquivo in arquivos:
        print(f"\nProcessando: {arquivo.name}")
        inicio = time.time()
        processar_planilha(arquivo)
        print(f"Concluído em {time.time() - inicio:.2f}s")

    save_cache()
    print("\nProcessamento concluído!")

if __name__ == "__main__":
    main()
