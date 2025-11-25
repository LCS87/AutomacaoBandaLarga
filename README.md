# Pipeline de Processamento e Enriquecimento de Dados de Prospecção (4 Etapas)

Este repositório contém um conjunto de scripts em Python projetados para automatizar e otimizar um pipeline de dados de prospecção (leads/prospects). O objetivo é pegar dados brutos, filtrá-los, enriquecê-los com informações de CNPJ/empresa e validar os contatos (operadora de telefone), preparando-os para o time comercial em um formato segmentado.


## Arquitetura do Projeto

O pipeline é modular e sequencial, garantindo que cada etapa se baseie nos dados processados da etapa anterior, culminando na separação final por operadora de telefonia.

1. Separação e Classificação (prospect_separator.py)

Filtra os dados de prospecção com base na análise de crédito (ENRIQUECIMENTO) e separa-os em grupos estratégicos (MEI, Demais, etc.) em arquivos Excel.

2. Enriquecimento de CNPJ e Endereço (cnpj_consultor.py)

Consulta uma API externa (com controle de limite de taxa e cache) para enriquecer os dados com informações detalhadas da empresa (Razão Social, Endereço, Situação Cadastral), usando o CNPJ como chave.

3. Validação de Contato (operadora_checker.py - Versão anterior)

Esta etapa é substituída e aprimorada pelo Script 4, que realiza a separação final.

4. Separação Final por Operadora (final_separator_operadora.py)

Processa os números de telefone (incluindo correção inteligente do dígito '9' para celulares antigos) e, crucialmente, separa o resultado final em múltiplos arquivos CSV, um para cada operadora ou tipo de linha (VIVO, CLARO, LINHA FIXA, etc.).

Como Configurar e Rodar o Projeto

-----------------------------------------------------------------------------------------------------

1. Pré-requisitos

Você precisa ter o Python 3.x instalado e as seguintes bibliotecas:

pip install pandas requests tqdm phonenumbers openpyxl xlsxwriter

----------------------------------------------------------------------------------------------------
2. Estrutura de Pastas

O projeto utiliza diferentes pastas de entrada/saída em cada etapa. Garanta a existência das seguintes pastas:

.
├── prospect_separator.py
├── cnpj_consultor.py
├── final_separator_operadora.py (Novo Script 4)
├── dados_prospect/ (Entrada P1, Saída P1)
├── Consultar/ (Entrada P2)
├── Consultado/ (Saída P2)
├── entrada/ (Entrada P4)
├── saida/ (Saída P4 - CSVs separados por operadora)
└── Dados do cokpit/
└── Logs/ (Logs e Cache do P2)

----------------------------------------------------------------------------------------------------------------------
3. Configurações e Chaves

Script 1 (prospect_separator.py): MANDATÓRIO ajustar a variável CAMINHO_BASE_CODIGO para o caminho absoluto da pasta onde o código está.

Script 2 (cnpj_consultor.py): MANDATÓRIO preencher a API_KEY e a URL de consulta da API no topo do arquivo.


----------------------------------------------------------------------------------------------------------------------

Guia de Execução do Pipeline (Passo a Passo)

A execução deve ser feita sequencialmente para garantir o fluxo de dados correto.

Passo 1: Separação e Classificação

Coloque seus arquivos CSV brutos na pasta dados_prospect/.

Execute:
python prospect_separator.py

Próxima Etapa: Mova os novos arquivos Excel (Ex: Propect BA - MEI.xlsx) de dados_prospect/ para a pasta Consultar/.

--------------------------------------------------------------------------------------------------------------------
Passo 2: Enriquecimento de CNPJ

Certifique-se de que a chave da API está configurada.

Execute:
python cnpj_consultor.py
(A barra de progresso tqdm será exibida).

Próxima Etapa: Mova os arquivos Excel enriquecidos (Ex: Propect BA - MEI_consultado_20251125_140000.xlsx) da pasta Consultado/ para a pasta entrada/.

--------------------------------------------------------------------------------------------------------------------
Passo 3: Separação Final por Operadora

Coloque os arquivos do Passo 2 na pasta entrada/.

Execute o script final:
python final_separator_operadora.py

Resultado Final: A pasta saida/ conterá múltiplos arquivos CSV, um para cada categoria (Ex: Leads__VIVO.csv, Leads__LINHA_FIXA.csv, Leads__INVALIDO.csv), prontos para uso.

final_separator_operadora.py é uma ferramenta de pós-processamento de alta qualidade que transforma dados de contato brutos em informações acionáveis, incorporando inteligência específica para o tratamento de números de telefone do Brasil.
