import boto3
import pandas as pd
import pyarrow.parquet as pq
import io

# Configurar boto3 para acessar o S3
s3 = boto3.client('s3')

# Nome do bucket e caminho do arquivo
BUCKET_NAME = "challenge-fiap-02-pks"
FOLDER_NAME = "b3-data"
DATE_PARTITION = "2025-03-09"  # Mudar para a data do arquivo que quer visualizar
FILE_NAME = f"{FOLDER_NAME}/{DATE_PARTITION}/dados_ibov.parquet"

# Baixa o arquivo do S3 para um buffer de memória
response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
parquet_data = response['Body'].read()

# Converte o buffer para DataFrame Pandas
table = pq.read_table(io.BytesIO(parquet_data))
df = table.to_pandas()

# Exibir as 10 primeiras linhas
print(
f"""
BUCKET_NAME = {BUCKET_NAME}
FOLDER_NAME = {FOLDER_NAME}
DATE_PARTITION = {DATE_PARTITION}
""")
print(df.head(10))