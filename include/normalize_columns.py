import pandas as pd
import unidecode
import os

def normalizar_csv(nome_arquivo):
    try:
        # Caminho relativo baseado na raiz do projeto
        caminho_arquivo = os.path.join(os.getcwd(), 'dbt_airflow_snowflake', 'seeds', nome_arquivo)

        # Verificar se o arquivo existe
        if not os.path.exists(caminho_arquivo):
            print(f"Arquivo não encontrado: {caminho_arquivo}")
            return

        # Carregar o arquivo, ignorando a segunda linha de cabeçalho (linhas de unidades)
        print(f"Carregando arquivo: {caminho_arquivo}")
        
        # Ler o arquivo ignorando a segunda linha de cabeçalho
        df = pd.read_csv(caminho_arquivo, delimiter=";", encoding="utf-8", header=0)

        # Normalizar os nomes das colunas: remove acentuação, converte para minúsculas, troca espaços por underscores e remove pontos
        print("Normalizando colunas...")
        df.columns = [unidecode.unidecode(col).strip().lower().replace(" ", "_").replace(".", "") for col in df.columns]
        
        df.drop(index=0, inplace=True, axis=0)

         # Substituir vírgula por ponto nas colunas numéricas
        for coluna in df.select_dtypes(include=['object']).columns:
            if df[coluna].str.contains(',').any():  # Verifica se a coluna contém vírgulas
                df[coluna] = df[coluna].str.replace(',', '.').astype(float)  # Substitui vírgula por ponto e converte para float

        # Exibir as primeiras linhas do dataframe para verificar se a normalização foi bem-sucedida
        print("Visualizando as primeiras linhas após normalização:")
        print(df.head())

        # Salvar o arquivo com as colunas normalizadas
        df.to_csv(caminho_arquivo, index=False)
        print(f"Arquivo {caminho_arquivo} normalizado com sucesso!")

    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo: {e}")

# Exemplo de como chamar a função com um caminho relativo
normalizar_csv('dados_estacao_gsc.csv')