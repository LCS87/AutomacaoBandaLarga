import pandas as pd
from tqdm import tqdm
import os
import sys
import logging
from pathlib import Path

# Importa função compartilhada (evita duplicação com Script 3)
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.telefone import verificar_operadora

tqdm.pandas()

# --- Configurações ---
SCRIPT_DIR    = Path(__file__).parent
INPUT_FOLDER  = SCRIPT_DIR / "entrada"
OUTPUT_FOLDER = SCRIPT_DIR / "saida"
TELEFONE_COLUMN_HINT = "TELEFONE"

COLUNAS_DESEJADAS = [
    'CNPJ', 'RazaoSocial', 'EnderecoCompleto',
    'Email', 'Operadora', 'Telefone'
]

INPUT_FOLDER.mkdir(exist_ok=True)
OUTPUT_FOLDER.mkdir(exist_ok=True)

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(SCRIPT_DIR / "separador_operadora.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def processar_e_separar_arquivo(caminho_arquivo: Path):
    """Lê Excel, aplica verificação de operadora e separa em CSVs por operadora."""
    logger.info(f"Processando: {caminho_arquivo.name}")

    try:
        df = pd.read_excel(str(caminho_arquivo), dtype=str)
    except Exception as e:
        logger.error(f"Erro ao ler {caminho_arquivo.name}: {e}")
        return

    # Localiza coluna de telefone (case-insensitive)
    coluna_telefone = next(
        (col for col in df.columns if TELEFONE_COLUMN_HINT.lower() in col.lower()),
        None
    )

    if not coluna_telefone:
        logger.warning(f"Coluna '{TELEFONE_COLUMN_HINT}' não encontrada em {caminho_arquivo.name}.")
        return

    # Valida colunas esperadas na saída (avisa sobre as ausentes)
    colunas_base = [c for c in COLUNAS_DESEJADAS if c not in ('Operadora', 'Telefone')]
    ausentes = [c for c in colunas_base if c not in df.columns]
    if ausentes:
        logger.warning(f"Colunas ausentes (serão ignoradas): {ausentes}")

    logger.info(f"Consultando operadora para {len(df)} linhas...")
    df['Operadora'] = df[coluna_telefone].progress_apply(verificar_operadora)

    operadoras_unicas = df['Operadora'].unique()
    logger.info(f"Separando em {len(operadoras_unicas)} grupos de operadora...")

    # Colunas disponíveis para extração (sem 'Telefone' — será renomeada)
    colunas_para_extracao = [c for c in COLUNAS_DESEJADAS if c not in ('Operadora', 'Telefone') and c in df.columns]
    colunas_para_extracao += ['Operadora', coluna_telefone]

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
        except Exception as e:
            logger.error(f"Erro ao salvar {nome_saida}: {e}")

    logger.info(f"Concluído: {caminho_arquivo.name}")


def main():
    arquivos = [f for f in INPUT_FOLDER.iterdir() if f.suffix in ('.xlsx', '.xls')]

    if not arquivos:
        logger.warning(f"Nenhum arquivo Excel encontrado em '{INPUT_FOLDER}'.")
        logger.info(f"Coloque seus arquivos .xlsx ou .xls em: {INPUT_FOLDER.resolve()}")
        return

    logger.info(f"Encontrados {len(arquivos)} arquivos. Saída em: {OUTPUT_FOLDER.resolve()}")

    for arquivo in arquivos:
        processar_e_separar_arquivo(arquivo)

    logger.info("Todos os arquivos processados e separados por operadora.")


if __name__ == "__main__":
    main()
