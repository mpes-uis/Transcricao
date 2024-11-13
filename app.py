##### PACOTES #####

#Basicão e DB
import os
import pandas as pd
import configparser
import pyodbc

#video e upload
from moviepy.editor import VideoFileClip
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
from moviepy.editor import VideoFileClip
import subprocess
# É necessário instalar o codec ffmpeg por fora na máquina ou conteiner que for rodar a aplicação

# transcrição
import logging
import sys
import requests
import time
import swagger_client
# O swagger_client é crucial e precisa ser instalado por fora
# Mais detalhes em https://github.com/Azure-Samples/cognitive-services-speech-sdk/tree/master/samples/batch/python


##### ENV VARS #####

import os
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = os.getenv('CONNECTION_STRING')
SERVICE_REGION = os.getenv('SERVICE_REGION')
SUBSCRIPTION_KEY = os.getenv('SUBSCRIPTION_KEY')
SQL_SERVER_CNXN_STR = os.getenv('SQL_SERVER_CNXN_STR')
SQL_SERVER_CNXN_STR_TSC = os.getenv('SQL_SERVER_CNXN_STR_TSC')
VIDEO_DIR = '/app/tsc/video'
AUDIO_DIR = '/app/tsc/audio'
EMPTY_TEXT = '503'


##### CONFIG #####

video_dir = VIDEO_DIR
audio_dir = AUDIO_DIR
texto_vazio = EMPTY_TEXT

##### EXTRAÇÃO DOS VIDEOS #####


def fila_transcricao():
    # Conexão ao SQL Server
    connection_str = os.getenv('SQL_SERVER_CNXN_STR_TSC')
    conn = pyodbc.connect(connection_str)

    # Query para buscar o primeiro id_documento_mni com campo 'dados' vazio
    query = """
    SELECT TOP 1 id_documento_mni
    FROM MPES_DM1.dbo.transcricoes
    WHERE dados IS NULL OR dados = ''
    ORDER BY id_documento_mni;
    """
    # Executa a query e retorna o valor único do DataFrame
    df = pd.read_sql_query(query, conn)
    
    # Fecha a conexão
    conn.close()

    # Retorna o valor único do DataFrame
    return df.iloc[0, 0] if not df.empty else None


def salvar_erro_no_banco(id_transcricao, texto_vazio):
    # Função para salvar o erro no banco de dados
    connection_str = os.getenv('SQL_SERVER_CNXN_STR_TSC')
    try:
        with conexao.cursor() as cursor:
            cursor.execute(
                "UPDATE MPES_DM1.dbo.transcricoes SET dados = ? WHERE id = ?",
                texto_vazio, id_transcricao
            )
            cursor.execute(
                "UPDATE MPES_DM1.dbo.transcricoes SET status = ? WHERE id = ?",
                '-1', id_transcricao
            )
            conexao.commit()
        print("Erro '503' salvo no banco de dados.")
    except Exception as e:
        print("Erro ao salvar no banco de dados:", e)

resultado_do_input = fila_transcricao()
#resultado_do_input = input("Digite o valor do id_documento_mni: ")

# Verificação se a função retornou None
if resultado_do_input is None:
    print("Sem transcrições na fila")
    exit()  # Encerra o programa imediatamente
else:
    resultado_do_input = int(resultado_do_input)

print(resultado_do_input)

# Carregar variáveis do .env

subscription_key = os.getenv('SUBSCRIPTION_KEY')
service_region = os.getenv('SERVICE_REGION')
connection_str = os.getenv('SQL_SERVER_CNXN_STR')

# Função para conectar ao SQL Server
def get_connection():
    return pyodbc.connect(connection_str)


def video_nao_encontrado(id_documento_mni, mensagem):
    # Conexão ao SQL Server
    connection_str = os.getenv('SQL_SERVER_CNXN_STR_TSC')
    conn = pyodbc.connect(connection_str)
    cursor = conn.cursor()

    # Query de atualização
    query = """
    UPDATE MPES_DM1.dbo.transcricoes
    SET dados = ?
    WHERE id_documento_mni = ?;
    """

    query2 = """
    UPDATE MPES_DM1.dbo.transcricoes
    SET status = -1
    WHERE id_documento_mni = ?;
    """
    
    # Executa a query com os parâmetros
    cursor.execute(query, mensagem, id_documento_mni)
    cursor.execute(query2, id_documento_mni)
    conn.commit()

    # Fecha a conexão
    cursor.close()
    conn.close()


# Função principal para baixar os arquivos
def download_files_from_db(doc_id, output_dir, texto_vazio):
    if doc_id is None:
        raise ValueError("O valor de 'id_documento_mni' não foi fornecido.")
    
    # Realizar a conexão ao banco de dados
    with get_connection() as conn:
        query = """
        SELECT au.IdAuto, au.Numero as numero_gampes, d.id as id_documento_mni, d.IdProcesso, 
               p.Numero as numeroPJE, a.mime_type, d.IdTipoDocumento, t.Descricao, vw.file_stream
        FROM MPES.mni.documentos d WITH (NOLOCK)
        INNER JOIN MPES.dbo.arquivos a WITH (NOLOCK) ON a.id = d.id_arquivo_original
        INNER JOIN GampesArqEfemeros.dbo.Arquivos vw WITH (NOLOCK) ON a.nome_unico_efemero = vw.name
        INNER JOIN [MPES].[MNI].[Processos] p WITH (NOLOCK) ON p.id = d.IdProcesso
        INNER JOIN MPES.gampes.[Auto] au WITH (NOLOCK) ON au.NumeroOrigemPrincipal = p.Numero
        LEFT JOIN MPES.MNI.TiposDocumentos t ON d.IdTipoDocumento = t.Id
        WHERE a.mime_type LIKE '%video/%' AND d.id = ?
        """
        df = pd.read_sql_query(query, conn, params=[doc_id])

    if df.empty:
        video_nao_encontrado(id_documento_mni=doc_id, mensagem=texto_vazio)
        print("DataFrame vazio. Nenhum arquivo encontrado para o id_documento_mni fornecido.")
        return


    # Certificar-se de que a pasta /output existe
    os.makedirs(output_dir, exist_ok=True)

    # Processar e salvar os arquivos
    for index, row in df.iterrows():
        file_name = f"{row['id_documento_mni']}.{row['mime_type'].split('/')[-1]}"  # Nome do arquivo
        file_path = os.path.join(output_dir, file_name)  # Caminho completo do arquivo
        
        # Salvando o arquivo em formato binário
        with open(file_path, 'wb') as f:
            f.write(row['file_stream'])

    print(f"Arquivos salvos com sucesso em {output_dir}!")


# Exemplo de execução, passando o valor do input:
download_files_from_db(doc_id=resultado_do_input, output_dir=video_dir, texto_vazio=texto_vazio)


##### CONVERSÃO PARA WAV #####

connection_string = os.getenv('CONNECTION_STRING')
service_region = os.getenv('SERVICE_REGION')
subscription_key = os.getenv('SUBSCRIPTION_KEY')

def extract_audio(input_filename):
    video_path = rf"/app/tsc/video/{input_filename}.mp4"
    audio_path = rf"/app/tsc/audio/{input_filename}.wav"
    
    # Command to extract audio using ffmpeg, with overwrite option (-y)
    command = [
        "ffmpeg",
        "-y",               # Overwrite output files without asking
        "-i", video_path,    # Arquivo de entrada
        "-ar", "16000",      # Taxa de amostragem
        "-ac", "1",          # Número de canais (mono)
        "-sample_fmt", "s16", # Formato de amostra (16 bits)
        audio_path           # Output file path
    ]

    try:
        # Execute the ffmpeg command
        subprocess.run(command, check=True)
        print(f"Audio extracted successfully to {audio_path}")
        return audio_path
    except subprocess.CalledProcessError as e:
        print(f"Error extracting audio: {e}\nOutput:\n{e.output}\nError:\n{e.stderr}")
        return None


##### UPLOAD E SAS TOKEN #####

# Function to upload a file to Azure Blob Storage
def upload_to_azure(audio_path, input_filename, connection_string):
    # Connect to the Azure Blob storage account
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "audio"
    
    # Create a blob client for the audio file
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{input_filename}.wav")
    
    # Upload the audio file to the 'audio' blob container
    with open(audio_path, "rb") as audio_file:
        blob_client.upload_blob(audio_file, overwrite=True)
    
    print(f"Audio file '{input_filename}.wav' uploaded to Azure Blob Storage.")
    return blob_client

# Function to generate a SAS token for the uploaded blob
def generate_sas_token(blob_client, input_filename, sas_expiry_hours=1):
    # Generate a SAS token for the uploaded file with read, list, and delete permissions
    sas_token = generate_blob_sas(
        account_name=blob_client.account_name,
        container_name=blob_client.container_name,
        blob_name=f"{input_filename}.wav",
        account_key=blob_client.credential.account_key,
        permission=BlobSasPermissions(read=True, list=True, delete=True),  # Allow read, list, and delete access
        expiry=datetime.utcnow() + timedelta(hours=sas_expiry_hours)  # Set expiry time for SAS
    )
    
    # Construct the full URL with SAS token
    sas_url = f"https://{blob_client.account_name}.blob.core.windows.net/{blob_client.container_name}/{input_filename}.wav?{sas_token}"
    
    print(f"SAS URL with read, list, and delete permissions: {sas_url}")
    return sas_url

def delete(input_filename, video_dir, audio_dir):
    video_path = rf"{video_dir}/{input_filename}.mp4"
    audio_path = rf"{audio_dir}/{input_filename}.wav"
        
    try:
        # Deletar o arquivo de vídeo se existir
        if os.path.exists(video_path):
            os.remove(video_path)
            print(f"Arquivo de vídeo '{video_path}' deletado com sucesso.")
        else:
            print(f"Arquivo de vídeo '{video_path}' não encontrado.")

        # Deletar o arquivo de áudio se existir
        if os.path.exists(audio_path):
            os.remove(audio_path)
            print(f"Arquivo de áudio '{audio_path}' deletado com sucesso.")
        else:
            print(f"Arquivo de áudio '{audio_path}' não encontrado.")

    except Exception as e:
            print(f"Ocorreu um erro ao tentar deletar os arquivos: {e}")


# Main function to run the full process
def extract_upload_and_generate_sas(input_filename, connection_string, video_dir, audio_dir, sas_expiry_hours=10,):
    # Step 1: Extract audio from the video
    audio_path = extract_audio(input_filename)
    
    # Step 2: Upload the audio to Azure Blob Storage
    blob_client = upload_to_azure(audio_path, input_filename, connection_string)
    
    # Step 2.5: Delete
    delete(input_filename, video_dir, audio_dir)

    # Step 3: Generate and return the SAS token
    return generate_sas_token(blob_client, input_filename, sas_expiry_hours)

# Usage example
# connection_string = "your-azure-connection-string"
sas_url = extract_upload_and_generate_sas(resultado_do_input, connection_string, video_dir, audio_dir)

"""
try:
    sas_url = extract_upload_and_generate_sas(resultado_do_input, connection_string, video_dir, audio_dir)
except Exception as e:
    # Caso ocorra algum erro, salva "503" no campo 'dados' do banco de dados
    salvar_erro_no_banco(resultado_do_input)
    print("Erro ao gerar URL SAS ou realizar upload:", e)

print(sas_url)

##### TRANSCRIÇÃO #####
"""

"""
Código baseado na referência do cognitive-services-speech-sdk
https://github.com/Azure-Samples/cognitive-services-speech-sdk/blob/master/samples/batch/python/python-client/main.py
É interessante futuramente fixar um modelo de referencia
Não são utilizadas todas funções, mas mantive elas no código para referência futura
Versão de Desenvolvimento rodando no RG-SpeechService-MPES
Versão de Produção rodando em RG-IA-Gampes
"""


connection_string = os.getenv('CONNECTION_STRING')
service_region = os.getenv('SERVICE_REGION')
subscription_key = os.getenv('SUBSCRIPTION_KEY')

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
        format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p %Z")


NAME = rf"transcription {resultado_do_input}"
DESCRIPTION = rf"transcricao do documento {resultado_do_input}"

LOCALE = "pt-BR"
RECORDINGS_BLOB_URI = sas_url

MODEL_REFERENCE = None  # guid of a custom model


def transcribe_from_single_blob(uri, properties):
    """
    Transcribe a single audio file located at `uri` using the settings specified in `properties`
    using the base model for the specified locale.
    """
    transcription_definition = swagger_client.Transcription(
        display_name=NAME,
        description=DESCRIPTION,
        locale=LOCALE,
        content_urls=[uri],
        properties=properties
    )

    return transcription_definition


def transcribe_with_custom_model(client, uri, properties):
    """
    Transcribe a single audio file located at `uri` using the settings specified in `properties`
    using the base model for the specified locale.
    """
    # Model information (ADAPTED_ACOUSTIC_ID and ADAPTED_LANGUAGE_ID) must be set above.
    if MODEL_REFERENCE is None:
        logging.error("Custom model ids must be set when using custom models")
        sys.exit()

    model = {'self': f'{client.configuration.host}/models/{MODEL_REFERENCE}'}

    transcription_definition = swagger_client.Transcription(
        display_name=NAME,
        description=DESCRIPTION,
        locale=LOCALE,
        content_urls=[uri],
        model=model,
        properties=properties
    )

    return transcription_definition


def transcribe_from_container(uri, properties):
    """
    Transcribe all files in the container located at `uri` using the settings specified in `properties`
    using the base model for the specified locale.
    """
    transcription_definition = swagger_client.Transcription(
        display_name=NAME,
        description=DESCRIPTION,
        locale=LOCALE,
        content_container_url=uri,
        properties=properties
    )

    return transcription_definition


def _paginate(api, paginated_object):
    """
    The autogenerated client does not support pagination. This function returns a generator over
    all items of the array that the paginated object `paginated_object` is part of.
    """
    yield from paginated_object.values
    typename = type(paginated_object).__name__
    auth_settings = ["api_key"]
    while paginated_object.next_link:
        link = paginated_object.next_link[len(api.api_client.configuration.host):]
        paginated_object, status, headers = api.api_client.call_api(link, "GET",
            response_type=typename, auth_settings=auth_settings)

        if status == 200:
            yield from paginated_object.values
        else:
            raise Exception(f"could not receive paginated data: status {status}")


def delete_all_transcriptions(api):
    """
    Delete all transcriptions associated with your speech resource.
    """
    logging.info("Deleting all existing completed transcriptions.")

    # get all transcriptions for the subscription
    transcriptions = list(_paginate(api, api.get_transcriptions()))

    # Delete all pre-existing completed transcriptions.
    # If transcriptions are still running or not started, they will not be deleted.
    for transcription in transcriptions:
        transcription_id = transcription._self.split('/')[-1]
        logging.debug(f"Deleting transcription with id {transcription_id}")
        try:
            api.delete_transcription(transcription_id)
        except swagger_client.rest.ApiException as exc:
            logging.error(f"Could not delete transcription {transcription_id}: {exc}")


def transcribe():
    logging.info("Starting transcription client...")

    # configure API key authorization: subscription_key
    configuration = swagger_client.Configuration()
    configuration.api_key["Ocp-Apim-Subscription-Key"] = subscription_key
    configuration.host = f"https://{service_region}.api.cognitive.microsoft.com/speechtotext/v3.2"

    # create the client object and authenticate
    client = swagger_client.ApiClient(configuration)

    # create an instance of the transcription api class
    api = swagger_client.CustomSpeechTranscriptionsApi(api_client=client)

    # Specify transcription properties by passing a dict to the properties parameter. See
    # https://learn.microsoft.com/azure/cognitive-services/speech-service/batch-transcription-create?pivots=rest-api#request-configuration-options
    # for supported parameters.
    properties = swagger_client.TranscriptionProperties()
    properties.word_level_timestamps_enabled = True
    properties.display_form_word_level_timestamps_enabled = True
    # properties.punctuation_mode = "DictatedAndAutomatic"
    # properties.profanity_filter_mode = "Masked"
    # properties.destination_container_url = "https://satranscricao.blob.core.windows.net/saida?sp=rw&st=2024-10-03T21:35:30Z&se=2024-10-04T05:35:30Z&spr=https&sv=2022-11-02&sr=c&sig=WjvePU61EScRVpyl%2F8%2BEPoBZJFNa0uBX6sFcBDScdTU%3D"
    # properties.time_to_live = "PT1H"

    # uncomment the following block to enable and configure speaker separation
    properties.diarization_enabled = True
    properties.diarization = swagger_client.DiarizationProperties(
         swagger_client.DiarizationSpeakersProperties(min_count=1, max_count=10))

    # uncomment the following block to enable and configure language identification prior to transcription. Available modes are "single" and "continuous".
    # properties.language_identification = swagger_client.LanguageIdentificationProperties(mode="single", candidate_locales=["en-US", "ja-JP"])

    # Use base models for transcription. Comment this block if you are using a custom model.
    transcription_definition = transcribe_from_single_blob(RECORDINGS_BLOB_URI, properties)

    # Uncomment this block to use custom models for transcription.
    # transcription_definition = transcribe_with_custom_model(client, RECORDINGS_BLOB_URI, properties)

    # uncomment the following block to enable and configure language identification prior to transcription
    # Uncomment this block to transcribe all files from a container.
    #transcription_definition = transcribe_from_container(RECORDINGS_CONTAINER_URI, properties)

    created_transcription, status, headers = api.transcriptions_create_with_http_info(transcription=transcription_definition)

    # get the transcription Id from the location URI
    transcription_id = headers["location"].split("/")[-1]

    # Log information about the created transcription. If you should ask for support, please
    # include this information.
    logging.info(f"Created new transcription with id '{transcription_id}' in region {service_region}")

    logging.info("Checking status.")

    completed = False

    while not completed:
        # wait for 5 seconds before refreshing the transcription status
        time.sleep(5)

        transcription = api.transcriptions_get(transcription_id)
        logging.info(f"Transcriptions status: {transcription.status}")

        if transcription.status in ("Failed", "Succeeded"):
            completed = True

        if transcription.status == "Succeeded":
            if properties.destination_container_url is not None:
                logging.info("Transcription succeeded. Results are located in your Azure Blob Storage.")
                break

            pag_files = api.transcriptions_list_files(transcription_id)
            for file_data in _paginate(api, pag_files):
                if file_data.kind != "Transcription":
                    continue

                audiofilename = file_data.name
                results_url = file_data.links.content_url
                results = requests.get(results_url)
                logging.info(f"Results for {audiofilename}:\n{results.content.decode('utf-8')}")
                return results
        elif transcription.status == "Failed":
            logging.info(f"Transcription failed: {transcription.properties.error.message}")


if __name__ == "__main__":
    transcription = transcribe()

transcricao = transcription.content.decode('utf-8')

##### GRAVANDO TRANSCRIÇÃO #####

def update_transcricao(id_documento_mni, transcricao):
    # Conexão ao SQL Server
    connection_str = os.getenv('SQL_SERVER_CNXN_STR_TSC')
    conn = pyodbc.connect(connection_str)
    cursor = conn.cursor()

    # Query de atualização
    query1 = """
    UPDATE MPES_DM1.dbo.transcricoes
    SET dados = ?
    WHERE id_documento_mni = ?;
    """
    query2 = """
    UPDATE MPES_DM1.dbo.transcricoes
    SET status = 1
    WHERE id_documento_mni = ?;
    """
    
    # Executa a query com os parâmetros
    cursor.execute(query1, transcricao, id_documento_mni)
    cursor.execute(query2, id_documento_mni)
    conn.commit()

    # Fecha a conexão
    cursor.close()
    conn.close()

# Exemplo de uso

id_documento_mni = resultado_do_input

update_transcricao(id_documento_mni, transcricao)

print("job completo")
