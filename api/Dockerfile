# Imagem da API de predição do IDEB (Cloud Run).
# O modelo NÃO vai dentro da imagem: a API baixa o artefato de MODELO_URI (gs://...) ao subir,
# então trocar a versão do modelo é só mudar a variável, sem reconstruir a imagem.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TF_CPP_MIN_LOG_LEVEL=2 \
    TF_ENABLE_ONEDNN_OPTS=0
WORKDIR /srv
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY app ./app
# O Cloud Run define PORT (8080 por padrão). Um worker: o TensorFlow ocupa memória por processo,
# e as requisições concorrentes são atendidas pelas threads do próprio servidor.
CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1 --timeout-keep-alive 75
