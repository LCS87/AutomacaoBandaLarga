import pandas as pd
import re
import os
import glob
import sys
from collections import defaultdict

# --- CONFIGURAÇÃO DE CAMINHOS (ajuste para seu ambiente) ---
# Caminho base do projeto (raw string para evitar problemas com \)
CAMINHO_BASE_CODIGO = r"C:\Users\avant\Documents\VScode\1 - SeparacaoProspect"

# Pasta de entrada e saída (subpasta dentro do caminho base)
PASTA_DADOS_ENTRADA = os.path.join(CAMINHO_BASE_CODIGO, "dados_prospect")
PASTA_DADOS_SAIDA = PASTA_DADOS_ENTRADA

# Garante que a pasta de saída exista
os.makedirs(PASTA_DADOS_SAIDA, exist_ok=True)


# --- FUNÇÕES DE SUPORTE ---

def extrair_analise_credito(enriquecimento: str) -> str:
    """Extrai o valor após 'ANALISE_CREDITO=' e remove caracteres extras."""
    if pd.isna(enriquecimento) or not isinstance(enriquecimento, str):
        return "N/A"

    match = re.search(r"ANALISE\_CREDITO\=(.*?)(?:\||$)", enriquecimento)
    if match:
        extracted = match.group(1).strip()
        # Remove o ponto final se for o último caractere
        if extracted.endswith('.'):
            extracted = extracted[:-1]
        return extracted.strip()
    return "N/A"


def classificar_cliente(descricao: str) -> str:
    """
    Classifica a descrição em 'MEI' ou 'DEMAIS' com base nas regras fornecidas.
    """
    if not isinstance(descricao, str):
        return "DESCARTAR"

    # GRUPO MEI: APROVADOS (Sem ponto final)
    mei_aprovados = {
        "Cliente MEI com alta probabilidade de aprovação",
        "Cliente MEI com média probabilidade de aprovação",
        "Cliente MEI com altissíma probabilidade de aprovação"
    }

    # GRUPO DEMAIS CLIENTES: APROVADOS E BOAS PROBABILIDADES (Sem ponto final)
    demais_clientes_aprovados = {
        "Cliente com altissíma probabilidade de aprovação",
        "Cliente com média probabilidade de aprovação",
        "Cliente aprovado até R$ 1700.00",
        "Cliente aprovado até R$ 3000.00",
        "Cliente aprovado até R$ 4000.00",
        "Cliente aprovado até R$ 1500.00",
        "Cliente aprovado com mais de R$ 5000,00",
        "Cliente aprovado até R$ 5000.00"
    }

    if descricao in mei_aprovados:
        return "MEI"
    if descricao in demais_clientes_aprovados:
        return "DEMAIS"
    return "DESCARTAR"


def salvar_dataframe_excel(df: pd.DataFrame, caminho_saida: str, nome_aba_resumo: str, total_linhas: int, df_resumo: pd.DataFrame):
    """Salva o DataFrame e o resumo em um único arquivo Excel, tentando engines diferentes se necessário."""
    # Garante diretório de saída
    os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)

    # Tenta usar xlsxwriter, se não disponível tenta openpyxl, se não disponível salva como CSV
    try:
        # Tenta explicitamente xlsxwriter primeiro
        with pd.ExcelWriter(caminho_saida, engine='xlsxwriter') as writer:
            if 'CHAVE_AGRUPAMENTO' in df.columns:
                df_para_salvar = df.drop(columns=['CHAVE_AGRUPAMENTO'])
            else:
                df_para_salvar = df
            df_para_salvar.to_excel(writer, sheet_name=nome_aba_resumo, index=False)
            df_resumo.to_excel(writer, sheet_name="Resumo_Contagem", index=False)
        print(f"  -> {os.path.basename(caminho_saida)} criado com {total_linhas} linhas.")
        return
    except Exception as e_xlsxwriter:
        # Se falhar por falta de módulo ou outro erro, tenta openpyxl
        try:
            with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
                if 'CHAVE_AGRUPAMENTO' in df.columns:
                    df_para_salvar = df.drop(columns=['CHAVE_AGRUPAMENTO'])
                else:
                    df_para_salvar = df
                df_para_salvar.to_excel(writer, sheet_name=nome_aba_resumo, index=False)
                df_resumo.to_excel(writer, sheet_name="Resumo_Contagem", index=False)
            print(f"  -> {os.path.basename(caminho_saida)} criado com {total_linhas} linhas (engine openpyxl).")
            return
        except Exception as e_openpyxl:
            # Se também falhar, salva como CSV como fallback
            try:
                csv_saida = os.path.splitext(caminho_saida)[0] + ".csv"
                if 'CHAVE_AGRUPAMENTO' in df.columns:
                    df_para_salvar = df.drop(columns=['CHAVE_AGRUPAMENTO'])
                else:
                    df_para_salvar = df
                df_para_salvar.to_csv(csv_saida, index=False, sep=';', encoding='utf-8-sig')
                resumo_csv = os.path.splitext(caminho_saida)[0] + "_resumo.csv"
                df_resumo.to_csv(resumo_csv, index=False, sep=';', encoding='utf-8-sig')
                print(f"  -> Falha ao criar .xlsx ({e_xlsxwriter} | {e_openpyxl}). Fallback: arquivos CSV criados: {os.path.basename(csv_saida)} e {os.path.basename(resumo_csv)}")
                return
            except Exception as e_csv:
                print(f"  ERRO ao escrever o arquivo {caminho_saida}: {e_csv}")
                return


def extrair_sigla_uf(nome_arquivo_base: str) -> str:
    """
    Tenta extrair a sigla da UF a partir do nome do arquivo.
    Estratégia:
      1) procura por duas letras maiúsculas isoladas (ex: ' BA ')
      2) procura por padrão ' - XX' ou ' XX -' etc.
      3) fallback: UF_INDEFINIDA
    """
    if not isinstance(nome_arquivo_base, str):
        return "UF_INDEFINIDA"

    # 1) procura por duas letras maiúsculas isoladas (palavra)
    m = re.search(r"\b([A-Z]{2})\b", nome_arquivo_base)
    if m:
        return m.group(1)

    # 2) procura por padrões comuns como ' - XX' ou 'XX -'
    m2 = re.search(r"[-_]\s*([A-Z]{2})\b", nome_arquivo_base)
    if m2:
        return m2.group(1)

    return "UF_INDEFINIDA"


def processar_arquivo_prospect(caminho_arquivo_entrada: str, caminho_saida: str):
    """
    Lê um arquivo CSV, filtra os dados em MEI e DEMAIS CLIENTES,
    e salva cada grupo em seu próprio arquivo Excel de saída.
    """
    nome_arquivo_base = os.path.basename(caminho_arquivo_entrada).replace(".csv", "").strip()
    uf_sigla = extrair_sigla_uf(nome_arquivo_base)

    print(f"\n--- Processando {nome_arquivo_base} (UF: {uf_sigla}) ---")

    try:
        # Leitura dos dados do CSV com encoding compatível (latin1)
        df_origem = pd.read_csv(caminho_arquivo_entrada, delimiter=';', encoding='latin1')
    except Exception as e:
        print(f"  ERRO ao ler o arquivo {caminho_arquivo_entrada}: {e}")
        return

    if 'ENRIQUECIMENTO' not in df_origem.columns:
        print(f"  AVISO: Coluna 'ENRIQUECIMENTO' não encontrada em {os.path.basename(caminho_arquivo_entrada)}. Pulando.")
        return

    # EXTRAÇÃO, FILTRAGEM E CLASSIFICAÇÃO
    df_origem['CHAVE_AGRUPAMENTO'] = df_origem['ENRIQUECIMENTO'].apply(extrair_analise_credito)
    df_origem['CLASSIFICACAO'] = df_origem['CHAVE_AGRUPAMENTO'].apply(classificar_cliente)

    # Filtra apenas o que não for DESCARTAR
    df_filtrado = df_origem[df_origem['CLASSIFICACAO'] != 'DESCARTAR'].copy()

    if df_filtrado.empty:
        print("  AVISO: Nenhum cliente elegível encontrado após a filtragem. Nenhum arquivo será gerado.")
        return

    # GERA DATAFRAMES SEPARADOS
    df_mei = df_filtrado[df_filtrado['CLASSIFICACAO'] == 'MEI'].drop(columns=['CLASSIFICACAO'])
    df_demais = df_filtrado[df_filtrado['CLASSIFICACAO'] == 'DEMAIS'].drop(columns=['CLASSIFICACAO'])

    # PROCESSA E SALVA O GRUPO MEI (se houver dados)
    if not df_mei.empty:
        resumo_mei = df_mei['CHAVE_AGRUPAMENTO'].value_counts().reset_index()
        resumo_mei.columns = ["Descrição ANALISE_CREDITO", "Quantidade de Linhas"]

        nome_arquivo_mei = f"Propect {uf_sigla} - MEI.xlsx"
        caminho_saida_mei = os.path.join(caminho_saida, nome_arquivo_mei)
        salvar_dataframe_excel(
            df_mei,
            caminho_saida_mei,
            "Propect MEI",
            len(df_mei),
            resumo_mei
        )

    # PROCESSA E SALVA O GRUPO DEMAIS (se houver dados)
    if not df_demais.empty:
        resumo_demais = df_demais['CHAVE_AGRUPAMENTO'].value_counts().reset_index()
        resumo_demais.columns = ["Descrição ANALISE_CREDITO", "Quantidade de Linhas"]

        nome_arquivo_demais = f"Propect {uf_sigla} - Demais clientes.xlsx"
        caminho_saida_demais = os.path.join(caminho_saida, nome_arquivo_demais)
        salvar_dataframe_excel(
            df_demais,
            caminho_saida_demais,
            "Propect Demais",
            len(df_demais),
            resumo_demais
        )


# --- FUNÇÃO PRINCIPAL ---

def main():
    # Verifica se a pasta de entrada existe
    if not os.path.isdir(PASTA_DADOS_ENTRADA):
        print(f"ERRO: A pasta de dados de entrada não foi encontrada: {PASTA_DADOS_ENTRADA}")
        sys.exit(1)

    arquivos_csv = glob.glob(os.path.join(PASTA_DADOS_ENTRADA, "*.csv"))

    if not arquivos_csv:
        print(f"AVISO: Nenhum arquivo .csv encontrado na pasta: {PASTA_DADOS_ENTRADA}")
        sys.exit(0)

    print(f"Encontrados {len(arquivos_csv)} arquivos para processar. Iniciando separação por tipo de cliente...")

    for caminho_arquivo in arquivos_csv:
        processar_arquivo_prospect(caminho_arquivo, PASTA_DADOS_SAIDA)

    print("\n=============================================")
    print("Processamento em lote de todos os arquivos concluído!")
    print(f"Arquivos de saída salvos em: {os.path.abspath(PASTA_DADOS_SAIDA)}")
    print("=============================================")


if __name__ == "__main__":
    main()