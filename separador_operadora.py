import pandas as pd
from tqdm import tqdm
import os
import phonenumbers
from phonenumbers import carrier
from phonenumbers.phonenumberutil import number_type, PhoneNumberType
import time

# Permite que o Pandas use a barra de progresso
tqdm.pandas()

# --- Configurações ---
INPUT_FOLDER = "entrada" # Pasta onde você deve colocar seus arquivos Excel
OUTPUT_FOLDER = "saida"  # Pasta onde os arquivos CSV serão salvos, separados por operadora
# A coluna com o número de telefone bruto/completo para processamento
TELEFONE_COLUMN_HINT = "TELEFONE" 
# Ordem das colunas na saída
COLUNAS_DESEJADAS = [
    'CNPJ',
    'RazaoSocial',
    'EnderecoCompleto',
    'Email',
    'Operadora',
    'Telefone' # Será renomeado a partir da coluna TELEFONE_COLUMN_HINT
]


# --- Função de Verificação de Operadora (Corrigida e Robusta) ---
def verificar_operadora(numero):
    """
    Verifica a operadora de um número de telefone, corrigindo a lógica
    para números fixos de 10 dígitos e celulares incompletos no formato brasileiro.
    """
    if pd.isna(numero) or str(numero).strip() == "":
        return ""

    numero_str = str(numero).strip()

    # 1. Padronização do número
    if numero_str.startswith('+'):
        numero_completo = numero_str
        numero_limpo = ''.join(filter(str.isdigit, numero_str[1:]))
    else:
        numero_limpo = ''.join(filter(str.isdigit, numero_str))
        numero_completo = "+55" + numero_limpo

    if len(numero_limpo) < 10:
        return "Inválido (Curto)"

    parsed_number = None
    
    try:
        # 2. Primeira Tentativa de Parseamento (como está)
        parsed_number = phonenumbers.parse(numero_completo, "BR")

        if not phonenumbers.is_valid_number(parsed_number):
            # 3. Segunda Tentativa: Correção do '9' para celulares incompletos (10 dígitos)
            if len(numero_limpo) == 10 and numero_limpo[2] != '9':
                numero_limpo_corrigido = numero_limpo[:2] + '9' + numero_limpo[2:]
                numero_completo_corrigido = "+55" + numero_limpo_corrigido
                parsed_number_corrigido = phonenumbers.parse(numero_completo_corrigido, "BR")

                if phonenumbers.is_valid_number(parsed_number_corrigido):
                    parsed_number = parsed_number_corrigido
                else:
                    return "Inválido (Corrigido Falhou)"
            else:
                return "Inválido (Formato)"

    except Exception:
        return "Inválido (Erro Parsing)"

    # 4. Verificação da Operadora
    if parsed_number is None:
        return "Inválido (Erro Interno)"

    operadora = carrier.name_for_number(parsed_number, "pt")

    if not operadora:
        n_type = number_type(parsed_number)
        if n_type == PhoneNumberType.FIXED_LINE:
            return "Linha Fixa"
        elif n_type == PhoneNumberType.MOBILE:
            return "Celular (Operadora não identificada)"
        return "Operadora Não Encontrada"

    # Retorna o nome da operadora em MAIÚSCULAS para padronização no agrupamento
    return operadora.upper()


def processar_e_separar_arquivo(caminho_arquivo):
    """
    Lê um arquivo Excel, aplica a verificação de operadora,
    separa por operadora e salva em arquivos CSV.
    """
    nome_arquivo = os.path.basename(caminho_arquivo)
    print(f"\nProcessando arquivo: {nome_arquivo}")

    try:
        # Lendo como string para preservar CNPJ, TELEFONE e evitar notação científica
        df = pd.read_excel(caminho_arquivo, dtype=str)
    except Exception as e:
        print(f"Erro ao ler o arquivo {nome_arquivo}: {e}")
        return

    # 1. Identificar a Coluna de Telefone (bruta)
    coluna_telefone_bruto = None
    for col in df.columns:
        if TELEFONE_COLUMN_HINT.lower() in str(col).lower():
            coluna_telefone_bruto = col
            break

    if not coluna_telefone_bruto:
        print(f"Aviso: Coluna contendo '{TELEFONE_COLUMN_HINT}' não encontrada em {nome_arquivo}.")
        return
    
    # Prepara a lista final de colunas a serem extraídas
    colunas_para_extracao = [c for c in COLUNAS_DESEJADAS if c != 'Telefone']
    colunas_para_extracao.append(coluna_telefone_bruto)


    # 2. Aplicar a verificação de operadora
    print(f"Iniciando a consulta de operadora ({len(df)} linhas)...")
    df['Operadora'] = df[coluna_telefone_bruto].progress_apply(verificar_operadora)

    # 3. Iterar e salvar por operadora
    operadoras_unicas = df['Operadora'].unique()

    print(f"Separando e salvando dados por operadora (Total de {len(operadoras_unicas)} grupos)...")

    for operadora in operadoras_unicas:
        # Limpa o nome para o arquivo
        nome_operadora_limpo = "".join(c for c in str(operadora) if c.isalnum() or c in (' ', '_', '-')).strip().replace(' ', '_')
        
        # Filtra o DataFrame e cria uma cópia
        df_operadora = df[df['Operadora'] == operadora].copy()
        
        if df_operadora.empty:
            continue
        
        # Seleciona as colunas na ordem desejada
        df_saida = df_operadora[colunas_para_extracao]
        
        # Renomeia a coluna de telefone bruto para 'Telefone'
        df_saida = df_saida.rename(columns={coluna_telefone_bruto: 'Telefone'})

        # Cria o nome do arquivo de saída
        base_nome, _ = os.path.splitext(nome_arquivo)
        nome_saida = f"{base_nome}__{nome_operadora_limpo}.csv"
        caminho_saida = os.path.join(OUTPUT_FOLDER, nome_saida)
        
        # Salva como CSV (delimitador ';')
        try:
            df_saida.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
            print(f"  -> Salvo: {nome_saida} ({len(df_saida)} linhas)")
        except Exception as e:
            print(f"Erro ao salvar {nome_saida}: {e}")
            
    print(f"Concluído o processamento de {nome_arquivo}.")


def main():
    # Cria as pastas se não existirem
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Lista todos os arquivos Excel na pasta de entrada
    arquivos_excel = [os.path.join(INPUT_FOLDER, f) for f in os.listdir(INPUT_FOLDER) if f.endswith(('.xlsx', '.xls'))]

    if not arquivos_excel:
        print(f"Nenhum arquivo Excel encontrado na pasta '{INPUT_FOLDER}'.")
        print(f"Certifique-se de que seus arquivos .xlsx ou .xls estão lá.")
        return

    print(f"Encontrados {len(arquivos_excel)} arquivos para processar.")
    print(f"Os arquivos CSV de saída serão salvos na pasta '{OUTPUT_FOLDER}'.")

    for arquivo in arquivos_excel:
        processar_e_separar_arquivo(arquivo)

    print("\n---")
    print("✅ Todos os arquivos foram processados e separados por operadora!")
    print(f"Os arquivos de saída estão na pasta '{OUTPUT_FOLDER}'.")
    print("---")


if __name__ == "__main__":
    main()