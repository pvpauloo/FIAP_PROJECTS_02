import time
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from io import BytesIO

# Configurar boto3 para acessar o S3
s3 = boto3.client('s3')

# Nome do bucket e pasta base onde os arquivos serão salvos
BUCKET_NAME = "challenge-fiap-02-pks"
BASE_FOLDER = "b3-data"

# URL da página da B3 contendo a tabela do IBOV
URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

def scrape_b3_selenium():
    """Faz o scraping da página da B3 usando Selenium para capturar os dados renderizados via JavaScript."""
    
    # Configuração do Chrome headless (modo sem interface gráfica)
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Executa sem abrir janela
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # Inicializa o navegador com o WebDriver gerenciado automaticamente
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Abre a URL da B3
    driver.get(URL)

    # Espera um tempo para a página carregar completamente
    time.sleep(5)

    # Captura o HTML completo da página carregada
    html_content = driver.page_source

    # Fecha o navegador
    driver.quit()

    # Usa Pandas para extrair a tabela do HTML renderizado
    tables = pd.read_html(html_content)
    
    if not tables:
        raise Exception("Nenhuma tabela encontrada na página.")

    # Pegamos a primeira tabela (que normalmente contém os dados do IBOV)
    df = tables[0]

    # Adiciona uma coluna com a data de extração
    df["data_extracao"] = datetime.today().strftime('%Y-%m-%d')

    return df

def save_parquet_to_s3(df, bucket_name, base_folder):
    """Converte um DataFrame Pandas para Parquet e faz upload para o S3 com partição diária."""
    
    # Obtém a data atual para criar a partição
    today = datetime.today().strftime('%Y-%m-%d')
    s3_folder = f"{base_folder}/{today}/"

    # Converte o DataFrame para um formato Parquet
    table = pa.Table.from_pandas(df)

    # Salva o arquivo em um buffer de memória
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Nome do arquivo Parquet a ser salvo no S3
    file_name = f"{s3_folder}dados_ibov.parquet"

    # Faz o upload para o S3
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

    print(f"Arquivo salvo com sucesso no S3: s3://{bucket_name}/{file_name}")

if __name__ == "__main__":
    # Faz o scraping usando Selenium
    df_ibov = scrape_b3_selenium()

    # Salva no S3 em formato Parquet
    save_parquet_to_s3(df_ibov, BUCKET_NAME, BASE_FOLDER)
