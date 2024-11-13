# Use a imagem base do Python
FROM python:3.11-slim

# Instale o ffmpeg, dependências do sistema, e MS-SQLServer em uma única camada
RUN apt-get update && \
    apt-get install -y ffmpeg unixodbc unixodbc-dev curl gnupg2 unzip cron && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Defina o diretório de trabalho
WORKDIR /app

# Copie todos os arquivos para o diretório de trabalho /app
COPY . .

# Extraia o conteúdo do arquivo swagger-client.zip e remova o arquivo zip
RUN unzip swagger-client.zip -d . && \
    rm swagger-client.zip

# Instale as dependências Python e o cliente gerado
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install ./swagger-client

# Crie diretórios necessários com permissões
RUN mkdir -p /app/tsc/video /app/tsc/audio && \
    chmod -R 777 /app/tsc

# Configure o cron para executar o app.py a cada minuto
RUN echo '* * * * * python /app/app.py' > /etc/cron.d/app-cron && \
    chmod 0644 /etc/cron.d/app-cron && \
    crontab /etc/cron.d/app-cron

# Start cron e mantenha o container ativo
CMD ["cron", "-f"]
