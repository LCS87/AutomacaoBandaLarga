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

tqdm.pandas()

# --- Configurações ---
SCRIPT_DIR    = Path(__file__).parent
INPUT_FOLDER  = SCRIPT_DIR / "entrada"
OUTPUT_FOLDER = SCRIPT_DIR / "saida"
TELEFONE_COLUMN_HINT = "TELEFONE"
LOG_LEVEL    = os.getenv("LOG_LEVEL", "INFO").upper()
SCRIPT_NAME  = "4-arquivo-discadora"

COLUNAS_DESEJADAS = [
    'CNPJ', 'RazaoSocial', 'EnderecoCompleto',
    'Email', 'Operadora', 'Telefone'
]

INPUT_FOLDER.mkdir(exist_ok=True)
OUTPUT_FOLDER.mkdir(exist_ok=True)

# --- Logging ---
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(SCRIPT_DIR / "separador_operadora.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def processar_e_separar_arquivo(caminho_arquivo: Path, dry_run: bool = False):
    """Lê Excel, aplica verificação de operadora e separa em CSVs por operadora."""
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

    colunas_base = [c for c in COLUNAS_DESEJADAS if c not in ('Operadora', 'Telefone')]
    ausentes = [c for c in colunas_base if c not in df.columns]
    if ausentes:
        logger.warning(f"Colunas ausentes (serão ignoradas): {ausentes}")

    logger.info(f"Consultando operadora para {len(df)} linhas...")

    if dry_run:
        logger.info(f"[DRY-RUN] {caminho_arquivo.name}: {len(df)} linhas. Nenhum arquivo gerado.")
        return

    with AuditTimer(SCRIPT_NAME, arquivo=caminho_arquivo.name) as timer:
        timer.total = len(df)

        df['Operadora'] = df[coluna_telefone].progress_apply(verificar_operadora)

        operadoras_unicas = df['Operadora'].unique()
        logger.info(f"Separando em {len(operadoras_unicas)} grupos de operadora...")

        colunas_para_extracao = [c for c in COLUNAS_DESEJADAS if c not in ('Operadora', 'Telefone') and c in df.columns]
        colunas_para_extracao += ['Operadora', coluna_telefone]

        arquivos_gerados = 0
        for operadora in operadoras_unicas:
            df_op = df[df['Operadora'] == operadora][colunas_para_extracao].copy()
            if df_op.empty:
                continue
            df_op = df_op.rename(columns={coluna_telefone: 'Telefone'})
            nome_limpo = "".join(
                c for c in str(operadora) if c.isalnum() or c in (' ', '_', '-')
            ).strip().replace(' ', '_')
            nome_saida = f"{caminho_arquivo.stem}__{nome_limpo}.csv"
            caminho_saida = OUTPUT_FOLDER / nome_saida
            try:
                df_op.to_csv(str(caminho_saida), index=False, sep=';', encoding='utf-8-sig')
                logger.info(f"Salvo: {nome_saida} ({len(df_op)} linhas)")
                arquivos_gerados += 1
            except Exception as e:
                logger.error(f"Erro ao salvar {nome_saida}: {e}")
                registrar_erro(SCRIPT_NAME, f"Erro ao salvar: {e}", arquivo=nome_saida)

        timer.processados = arquivos_gerados
        timer.falhas = len(operadoras_unicas) - arquivos_gerados
        logger.info(f"Concluído: {caminho_arquivo.name}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Separação de contatos por operadora para discadora.")
    parser.add_argument("--dry-run", action="store_true", help="Valida inputs sem gerar arquivos de saída.")
    args = parser.parse_args()

    arquivos = [f for f in INPUT_FOLDER.iterdir() if f.suffix in ('.xlsx', '.xls')]

    if not arquivos:
        logger.warning(f"Nenhum arquivo Excel encontrado em '{INPUT_FOLDER}'.")
        logger.info(f"Coloque seus arquivos .xlsx ou .xls em: {INPUT_FOLDER.resolve()}")
        return

    logger.info(f"Encontrados {len(arquivos)} arquivos. Saída em: {OUTPUT_FOLDER.resolve()}")
    if args.dry_run:
        logger.info("[DRY-RUN] Nenhum arquivo será gerado.\n")

    for arquivo in arquivos:
        processar_e_separar_arquivo(arquivo, dry_run=args.dry_run)

    logger.info("Todos os arquivos processados e separados por operadora.")


if __name__ == "__main__":
    main()
