import os
import sys
import logging
import time
import concurrent.futures
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

# Resolve o pacote raiz sem sys.path.insert manual.
# Com `pip install -e .` na raiz, este import funciona diretamente.
# Fallback para execução direta sem instalação do pacote.
_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from domain.empresa import Empresa
from infra.cnpj_api_client import CnpjApiClient
from infra.cnpj_cache import CnpjCache
from utils.rate_limiter import RateLimiter
from utils.audit_logger import AuditTimer, registrar_erro

load_dotenv(_ROOT / ".env")

# ================== CONFIGURAÇÕES ==================
SCRIPT_DIR = Path(__file__).parent

INPUT_FOLDER  = SCRIPT_DIR / "Consultar"
OUTPUT_FOLDER = SCRIPT_DIR / "Consultado"
LOGS_FOLDER   = SCRIPT_DIR / "Dados do cokpit" / "Logs"
CACHE_FILE    = SCRIPT_DIR / "Dados do cokpit" / "cnpj_cache.json"

API_KEY  = os.getenv("CNPJ_API_KEY", "")
API_URL  = os.getenv("CNPJ_API_URL", "")

MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "900"))
REQUEST_TIMEOUT         = int(os.getenv("REQUEST_TIMEOUT", "10"))
MAX_WORKERS             = int(os.getenv("MAX_WORKERS", "10"))
CACHE_TTL_DAYS          = int(os.getenv("CACHE_TTL_DAYS", "30"))
CNPJ_COLUMN             = os.getenv("CNPJ_COLUMN", "DOCUMENTO")
LOG_LEVEL               = os.getenv("LOG_LEVEL", "INFO").upper()
SCRIPT_NAME             = "2-consulta-cnpj"

for folder in (INPUT_FOLDER, OUTPUT_FOLDER, LOGS_FOLDER, CACHE_FILE.parent):
    folder.mkdir(parents=True, exist_ok=True)

# ================== LOGGING ==================
logging.basicConfig(
    filename=str(LOGS_FOLDER / f"cnpj_consultas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ================== VALIDAÇÃO DE CONFIGURAÇÃO ==================
def _validar_configuracao() -> bool:
    """Valida credenciais obrigatórias no startup. Aborta antes de qualquer I/O."""
    erros = []
    if not API_KEY:
        erros.append("CNPJ_API_KEY não definida no .env")
    if not API_URL:
        erros.append("CNPJ_API_URL não definida no .env")
    if erros:
        for erro in erros:
            logger.error(f"[CONFIG] {erro}")
        print("\n[ERRO DE CONFIGURAÇÃO] O script não pode continuar:")
        for erro in erros:
            print(f"  - {erro}")
        print("Copie .env.example para .env e preencha os valores obrigatórios.\n")
        return False
    return True


# ================== HELPERS ==================
def format_cnpj(cnpj) -> str | None:
    if pd.isna(cnpj):
        return None
    digits = "".join(filter(str.isdigit, str(cnpj).strip()))
    return digits if len(digits) == 14 else None


# ================== LÓGICA DE CONSULTA ==================
def _consultar_cnpj(cnpj: str, client: CnpjApiClient, cache: CnpjCache) -> dict:
    """Consulta um CNPJ: tenta cache primeiro, depois API. Retorna dict para o DataFrame."""
    cached = cache.get(cnpj)
    if cached:
        return cached.to_dict()

    empresa = client.consultar(cnpj)
    if empresa:
        cache.set(cnpj, empresa)
        return empresa.to_dict()

    # Registra falha no cache para evitar re-consulta imediata
    falha = Empresa(cnpj=cnpj, razao_social="NÃO ENCONTRADO")
    cache.set(cnpj, falha, fonte="Erro")
    return falha.to_dict()


def processar_cnpjs(cnpjs: list[str], client: CnpjApiClient, cache: CnpjCache) -> list[dict]:
    """Processa lista de CNPJs: cache hit direto, novos via ThreadPoolExecutor."""
    resultados: list[dict] = []
    novos: list[str] = []

    for cnpj in cnpjs:
        cached = cache.get(cnpj)
        if cached:
            resultados.append(cached.to_dict())
        else:
            novos.append(cnpj)

    if novos:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_consultar_cnpj, c, client, cache): c for c in novos}
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

                # Flush periódico a cada 100 consultas
                if (i + 1) % 100 == 0:
                    cache.flush()

    return resultados


# ================== PROCESSAMENTO DE PLANILHAS ==================
def processar_planilha(arquivo: Path, client: CnpjApiClient, cache: CnpjCache, dry_run: bool = False) -> Path | None:
    logger.info(f"Iniciando: {arquivo.name}")

    try:
        if arquivo.suffix == ".xlsx":
            df = pd.read_excel(str(arquivo), dtype={CNPJ_COLUMN: str})
        else:
            df = pd.read_csv(str(arquivo), sep=";", dtype={CNPJ_COLUMN: str})
    except Exception as e:
        logger.error(f"Erro ao ler {arquivo.name}: {e}")
        registrar_erro(SCRIPT_NAME, f"Erro ao ler arquivo: {e}", arquivo=arquivo.name)
        return None

    if CNPJ_COLUMN not in df.columns:
        logger.error(f"Coluna '{CNPJ_COLUMN}' não encontrada em {arquivo.name}")
        return None

    df["CNPJ_FORMATADO"] = df[CNPJ_COLUMN].apply(format_cnpj)
    cnpjs = [c for c in df["CNPJ_FORMATADO"].unique() if c]

    if not cnpjs:
        logger.error(f"Nenhum CNPJ válido (14 dígitos) em {arquivo.name}")
        return None

    print(f"  CNPJs únicos para consulta: {len(cnpjs)}")

    if dry_run:
        logger.info(f"[DRY-RUN] {arquivo.name}: {len(df)} linhas, {len(cnpjs)} CNPJs únicos. Nenhuma consulta realizada.")
        return None

    with AuditTimer(SCRIPT_NAME, arquivo=arquivo.name) as timer:
        timer.total = len(cnpjs)

        resultados = processar_cnpjs(cnpjs, client, cache)
        df_resultado = pd.DataFrame(resultados)

        falhas_lista = df_resultado[
            df_resultado["RazaoSocial"].isin(["NÃO ENCONTRADO", "ERRO NA CONSULTA"])
        ]["CNPJ"].tolist()

        timer.processados = len(cnpjs) - len(falhas_lista)
        timer.falhas = len(falhas_lista)

        df_resultado = df_resultado.drop(columns=[c for c in ("_cached_at",) if c in df_resultado.columns])

        df_final = pd.merge(df, df_resultado, left_on="CNPJ_FORMATADO", right_on="CNPJ", how="left")
        df_final = df_final.drop(columns=["CNPJ_FORMATADO"])

        colunas_ordenadas = [
            "SMARTCODE", "DOCUMENTO", "CNPJ", "RazaoSocial", "NomeFantasia",
            "SituacaoCadastral", "DataAbertura", "NaturezaJuridica", "CapitalSocial",
            "AtividadePrincipal", "CEP", "Logradouro", "Numero", "Complemento",
            "Bairro", "Municipio", "UF", "EnderecoCompleto",
            "TELEFONE", "Telefone", "Email", "ENRIQUECIMENTO",
            "FLAGDISPO", "TIPO_BLOQUEIO_TELEFONE", "Fonte",
        ]
        df_final = df_final.reindex(columns=[c for c in colunas_ordenadas if c in df_final.columns])

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUT_FOLDER / f"{arquivo.stem}_consultado_{ts}.xlsx"
        df_final.to_excel(str(output_file), index=False)

        if falhas_lista:
            falhas_file = OUTPUT_FOLDER / f"{arquivo.stem}_falhas_{ts}.txt"
            falhas_file.write_text("\n".join(falhas_lista), encoding="utf-8")
            logger.warning(f"{len(falhas_lista)} CNPJs com falha salvos em {falhas_file.name}")

        logger.info(f"Concluído: {output_file.name}")
        print(f"  Salvo em: {output_file}")
        return output_file


# ================== MAIN ==================
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enriquecimento de CNPJ em lote.")
    parser.add_argument("--dry-run", action="store_true", help="Valida inputs sem consultar a API nem gerar arquivos.")
    args = parser.parse_args()

    if not _validar_configuracao():
        sys.exit(1)

    cache = CnpjCache(cache_file=CACHE_FILE, ttl_days=CACHE_TTL_DAYS)
    cache.load()

    rate_limiter = RateLimiter(max_requests=MAX_REQUESTS_PER_MINUTE, time_window=60.0)
    client = CnpjApiClient(
        api_key=API_KEY,
        api_url=API_URL,
        rate_limiter=rate_limiter,
        timeout=REQUEST_TIMEOUT,
    )

    arquivos = [
        f for f in INPUT_FOLDER.iterdir()
        if f.suffix in (".xlsx", ".csv") and not f.name.startswith("~")
    ]

    if not arquivos:
        print(f"Nenhum arquivo encontrado em {INPUT_FOLDER}")
        return

    print(f"\nArquivos encontrados: {len(arquivos)}")
    if args.dry_run:
        print("[DRY-RUN] Nenhuma consulta será realizada.\n")

    for arquivo in arquivos:
        print(f"\nProcessando: {arquivo.name}")
        inicio = time.time()
        processar_planilha(arquivo, client, cache, dry_run=args.dry_run)
        print(f"Concluído em {time.time() - inicio:.2f}s")

    cache.flush()
    print("\nProcessamento concluído!")


if __name__ == "__main__":
    main()
