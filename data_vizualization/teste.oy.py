import pandas as pd
from multiprocessing import Pool
from functools import partial
from sqlalchemy import String, Float, Date, MetaData,inspect
from sqlalchemy.engine.reflection import Inspector
from glob import glob

from plugins.utils.empresa import cast_date_columns, cast_float_columns, create_upsert_method, get_db_engine, create_id
from logging import getLogger
import logging

logger = getLogger(__name__)
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
fileHandler = logging.FileHandler("empresas.log")
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

def download_zips(logical_date):
    HTML = requests.get('https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj')
    urls = re.findall('http://.*?.zip', HTML.text)
    logger.info(f'Baixando a lista de zip {urls}')
    with Pool(10) as pool:
        pool.map(partial(download, logical_date=logical_date), urls)
def download(url, logical_date):
    content_size = int(requests.get(url, stream=True).headers['Content-Length'])
    BASE_PATH = '/opt/airflow/data/raw/receita/'
    filepath = os.path.join(BASE_PATH,
                            str(logical_date.year),
                            f'{logical_date.month:02d}')
    if os.path.exists(filepath):
        logger.info('Arquivos baixados')
        return 
    os.makedirs(filepath , exist_ok=True)
    
    filename = os.path.join(filepath, url.split('/')[-1])
    if os.path.exists(filename):
        temp_size = os.path.getsize(filename)
    else:
        temp_size = 0
    headers = {'Range': 'bytes=%d-' % temp_size}
    r = requests.get(url, stream=True, headers=headers)
    logger.info(f'Começou Download da url {url}')
    with open(filename, 'ab') as file:
        for data in r.iter_content(chunk_size=1024):
            file.write(data)
            temp_size += len(data)
    logger.info('\n' + 'Download Terminou!')
def extract(logical_date):
    BASE_PATH = '/opt/airflow/data/raw/receita/'
    filepath = os.path.join(BASE_PATH,
                            str(logical_date.year),
                            f'{logical_date.month:02d}')
    file_extract = os.path.join(filepath,'extract_files')    
    file_names =  glob(f'{filepath}/*.zip')
    if os.path.exists(file_extract):
        logger.info('Arquivos já extraídos')
        return 
    for item in file_names: 
        try:
            with zipfile.ZipFile(item) as zip_ref: 
                zip_ref.extractall(file_extract)
                logger.info(f'Arquivo {item} extraído com sucesso')
        except zipfile.BadZipFile as e:
            logger.exception(e)           
def save_to_db(logical_date):
    BASE_PATH = "/opt/airflow/data/raw/receita/"
    filepath = os.path.join(
        BASE_PATH, str(logical_date.year), f"{logical_date.month:02d}"
    )
    
    extracted_files_dir = os.path.join(filepath, 'extract_files') 
    files = os.listdir(extracted_files_dir)

    layout_files = {
        "EMPRE": {
            "columns": {
                "cnpj_base": String(),
                "razao_social": String(),
                "codigo_natureza_juridica": String(),
                "codigo_qualificacao": String(),
                "capital_social": Float(),
                "codigo_porte_empresa": String(),
                "ente_federativo": String(),
                "id": String()
            },
            "table_name_db": "empresas",
        },
        "SIMPLES": {
            "columns": {
                "cnpj_base": String(),
                "opcao_simples": String(),
                "data_opcao_simples": Date(),
                "data_exclusao_simples": Date(),
                "opcao_mei": String(),
                "data_opcao_mei": Date(),
                "data_exclusao_mei": Date(),
                "id": String()
            },
            "table_name_db": "dados_simples",
        },
        "SOCIO": {
            "columns": {
                "cnpj_base": String(),
                "codigo_tipo": String(),
                "nome": String(),
                "cpf_cnpj": String(),
                "codigo_qualificacao": String(),
                "data_entrada": Date(),
                "codigo_pais": String(),
                "representante": String(),
                "nome_representante": String(),
                "codigo_qualificacao_representante": String(),
                "codigo_faixa_etaria": String(),
                "id": String()
            },
            "table_name_db": "socios",
        },
        "ESTABELE": {
            "columns": {
                "cnpj_base": String(),
                "cnpj_ordem": String(),
                "cnpj_dv": String(),
                "codigo_matriz_filial": String(),
                "nome_fantasia": String(),
                "codigo_situacao_cadastral": String(),
                "data_situacao_cadastral": Date(),
                "codigo_motivo_situacao_cadastral": String(),
                "cidade_exterior": String(),
                "codigo_pais": String(),
                "data_inicio_atividade": Date(),
                "codigo_cnae_principal": String(),
                "codigo_cnae_secundario": String(),
                "tipo_logradouro": String(),
                "logradouro": String(),
                "numero": String(),
                "complemento": String(),
                "bairro": String(),
                "cep": String(),
                "uf": String(),
                "codigo_municipio": String(),
                "ddd1": String(),
                "telefone1": String(),
                "ddd2": String(),
                "telefone2": String(),
                "ddd_fax": String(),
                "fax": String(),
                "email": String(),
                "situacao_especial": String(),
                "data_situacao_especial": Date(),
                "id": String()
            },
            "table_name_db": "estabelecimentos",
        },
        "PAIS": {
            "columns": {"codigo_pais": String(), "pais": String()},
            "table_name_db": "paises",
        },
        "MUNIC": {
            "columns": {"codigo_municipio": String(), "municipio": String()},
            "table_name_db": "municipios",
        },
        "QUALS": {
            "columns": {"codigo_qualificacao": String(), "qualificacao": String()},
            "table_name_db": "qualificacoes_socios",
        },
        "NATJU": {
            "columns": {"codigo_natureza_juridica": String(), "natureza_juridica": String()},
            "table_name_db": "naturezas_juridicas",
        },
        "MOTI": {
            "columns": {
                "codigo_motivo_situacao_cadastral": String(),
                "motivo_situacao_cadastral": String(),
            },
            "table_name_db": "motivo_situacao_cadastral",
        },
        "CNAE": {
            "columns": {"codigo_cnae": String(), "cnae": String()},
            "table_name_db": "cnaes",
        },
    }

    models = list(layout_files.keys())

for file in files:
        logger.info(f'Arquivo {file} sendo inserido no banco empresas')
        print(f'Arquivo {file} sendo inserido no banco empresas')

@@ -216,39 +224,32 @@ def save_to_db(logical_date):
        upsert_method = create_upsert_method(meta)

 for df in pd.read_csv(
            f"{extracted_files_dir}/{file}",
            names=list(columns_dtypes.keys()),
            encoding="ISO-8859-1",

                        sep=";",
            chunksize=1000,
        ): 

            df = create_id(df,table_name)
            df = cast_date_columns(df, columns_dtypes)
            df = cast_float_columns(df, columns_dtypes)

            with engine.connect() as conn:
                try:  
                    df.to_sql(
                            name=table_name,
                            dtype=columns_dtypes,
                            con=conn,
                            index=False,
                            if_exists="append",
                            method=upsert_method
                            )
                except Exception as e:
                    print("Problema no to_sql\n",e)
        logger.info(f'Registros do arquivo {file} inseridos com sucesso')
        print(f'Registros do arquivo {file} inseridos com sucesso')

    engine.dispose()