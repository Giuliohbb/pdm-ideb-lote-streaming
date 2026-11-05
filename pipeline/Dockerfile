# Uma imagem para os dois componentes do pipeline:
#   * serviço de limpeza (Cloud Run Service): comando padrão abaixo
#   * construtor da gold (Cloud Run Job): sobrescreve o comando com "python construir_gold.py"
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY esquema.py regras_limpeza.py servico_limpeza.py sql_gold.py construir_gold.py colunas_bronze.json ./
CMD exec gunicorn --bind :$PORT --workers 1 --threads 16 --timeout 0 servico_limpeza:app
