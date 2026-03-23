import pandas as pd
from tqdm import tqdm
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / ".env")

from utils.telefone import verificar_operadora
from utils.audit_logger import AuditTimer, registrar_erro

# --- Configurações ---
SCRIPT_DIR    = Path(__file__).parent
INPUT_FOLDER  = SCRIPT_DIR / "arquivos_de_entrada"
OUTPUT_FOLDER = SCRIPT_DIR / "arquivos_com_operadora"
TELEFONE_COLUMN_HINT = "telefone"
LOG_LEVEL    = os.getenv("LOG_LEVEL", "INFO").upper()
SCRIPT_NAME  = "3-consulta-operadora"

INPUT_FOLDER.mkdir(exist_ok=True)
OUTPUT_FOLDER.mkdir(exist_ok=True)

# --- Logging ---
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(SCRIPT_DIR / "consulta_operadora.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def processar_arquivo_excel(caminho_arquivo: Path, dry_run: bool = False):
    """Lê um arquivo Excel, identifica operadora de cada telefone e salva o resultado."""
    logger.info(f"Processando: {caminho_arquivo.name}")

    try:
        df = pd.read_excel(str(caminho_arquivo), dtype=str)
    except Exception as e:
        logger.error(f"Erro ao ler {caminho_arquivo.name}: {e}")
        registrar_erro(SCRIPT_NAME, f"Erro ao ler arquivo: {e}", arquivo=caminho_arquivo.name)
        return

    coluna_telefone = next(
        (col for col in df.columns if TELEFONE_COLUMN_HINT.lower() in col.lower()),
        None
    )

    if not coluna_telefone:
        logger.warning(f"Coluna '{TELEFONE_COLUMN_HINT}' não encontrada em {caminho_arquivo.name}.")
        return

    logger.info(f"Consultando {len(df)} números na coluna '{coluna_telefone}'...")

    if dry_run:
        logger.info(f"[DRY-RUN] {caminho_arquivo.name}: {len(df)} linhas. Nenhum arquivo gerado.")
        return

    with AuditTimer(SCRIPT_NAME, arquivo=caminho_arquivo.name) as timer:
        timer.total = len(df)
        df['Operadora'] = df[coluna_telefone].apply(verificar_operadora)
        df['Operadora'] = df['Operadora'].astype(str)
        timer.processados = len(df)
        timer.falhas = int(df['Operadora'].str.startswith('Inválido').sum())

        nome_saida = f"{caminho_arquivo.stem}_com_operadora{caminho_arquivo.suffix}"
        caminho_saida = OUTPUT_FOLDER / nome_saida

        try:
            df.to_excel(str(caminho_saida), index=False)
            logger.info(f"Salvo em: {caminho_saida}")
        except Exception as e:
            logger.error(f"Erro ao salvar {nome_saida}: {e}. Verifique se o arquivo não está aberto.")
            registrar_erro(SCRIPT_NAME, f"Erro ao salvar: {e}", arquivo=nome_saida)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Consulta de operadora de telefone.")
    parser.add_argument("--dry-run", action="store_true", help="Valida inputs sem gerar arquivos de saída.")
    args = parser.parse_args()

    arquivos = [f for f in INPUT_FOLDER.iterdir() if f.suffix in ('.xlsx', '.xls')]

    if not arquivos:
        logger.warning(f"Nenhum arquivo Excel encontrado em '{INPUT_FOLDER}'.")
        return

    logger.info(f"Encontrados {len(arquivos)} arquivos para processar.")
    if args.dry_run:
        logger.info("[DRY-RUN] Nenhum arquivo será gerado.\n")

    for arquivo in arquivos:
        processar_arquivo_excel(arquivo, dry_run=args.dry_run)

    logger.info("Todos os arquivos foram processados.")


if __name__ == "__main__":
    main()
