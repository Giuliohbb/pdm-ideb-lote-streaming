#!/usr/bin/env bash
# =============================================================================
# Implanta o pipeline de streaming do Trabalho 2 no GCP.
# Rode no Cloud Shell, a partir da pasta raiz do projeto (onde estão pipeline/, sql/ etc.):
#
#   bash infra/implantar.sh
#
# O script é idempotente: pode ser rodado de novo sem duplicar recursos.
# Ajuste as variáveis abaixo se o projeto, a região ou o dataset forem outros.
# =============================================================================
set -euo pipefail

PROJETO="${PROJETO:-platinum-avenue-505512-t5}"
REGIAO="${REGIAO:-us-central1}"
DATASET="${DATASET:-trabalho_pdm}"
MODELO="${MODELO:-modelo_ideb}"

TOPICO="ideb-bronze-eventos"
TOPICO_DLQ="ideb-bronze-dlq"
SUB_PUSH="sub-limpeza-push"
SUB_BQ="sub-bronze-bigquery"
SUB_DLQ="sub-dlq-inspecao"
SERVICO="limpeza-ideb"
JOB_GOLD="gold-ideb"
REPO="pdm-trabalho2"
IMAGEM="${REGIAO}-docker.pkg.dev/${PROJETO}/${REPO}/pipeline-ideb:latest"

SA_LIMPEZA="sa-limpeza-ideb@${PROJETO}.iam.gserviceaccount.com"
SA_PUSH="sa-pubsub-push@${PROJETO}.iam.gserviceaccount.com"
SA_GOLD="sa-gold-ideb@${PROJETO}.iam.gserviceaccount.com"

passo() { echo; echo "==> $*"; }

gcloud config set project "$PROJETO" >/dev/null
NUM_PROJETO=$(gcloud projects describe "$PROJETO" --format='value(projectNumber)')
AGENTE_PUBSUB="service-${NUM_PROJETO}@gcp-sa-pubsub.iam.gserviceaccount.com"

passo "1/8 Habilitando APIs"
gcloud services enable pubsub.googleapis.com run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com bigquery.googleapis.com iam.googleapis.com
gcloud beta services identity create --service=pubsub.googleapis.com --project="$PROJETO" >/dev/null 2>&1 || true

passo "2/8 Criando tabelas e views de streaming no BigQuery (dataset ${DATASET})"
bq query --project_id="$PROJETO" --use_legacy_sql=false < sql/01_tabelas_stream.sql >/dev/null
bq query --project_id="$PROJETO" --use_legacy_sql=false < sql/02_views.sql >/dev/null

passo "3/8 Contas de serviço e permissões"
criar_sa() {
  gcloud iam service-accounts describe "$1@${PROJETO}.iam.gserviceaccount.com" >/dev/null 2>&1 \
    || gcloud iam service-accounts create "$1" --display-name="$2"
}
criar_sa sa-limpeza-ideb "Pipeline IDEB - servico de limpeza"
criar_sa sa-pubsub-push "Pipeline IDEB - identidade do push do Pub/Sub"
criar_sa sa-gold-ideb "Pipeline IDEB - job da gold"
sleep 10   # contas recém-criadas demoram alguns segundos para aceitar permissões
for SA in "$SA_LIMPEZA" "$SA_GOLD"; do
  for PAPEL in roles/bigquery.dataEditor roles/bigquery.jobUser; do
    gcloud projects add-iam-policy-binding "$PROJETO" --member="serviceAccount:${SA}" \
      --role="$PAPEL" --condition=None >/dev/null
  done
done
# O agente do Pub/Sub grava a bronze crua (assinatura BigQuery) e assina o token OIDC do push
gcloud projects add-iam-policy-binding "$PROJETO" --member="serviceAccount:${AGENTE_PUBSUB}" \
  --role=roles/bigquery.dataEditor --condition=None >/dev/null
gcloud iam service-accounts add-iam-policy-binding "$SA_PUSH" \
  --member="serviceAccount:${AGENTE_PUBSUB}" --role=roles/iam.serviceAccountTokenCreator >/dev/null

passo "4/8 Tópicos do Pub/Sub (principal e dead letter)"
gcloud pubsub topics describe "$TOPICO" >/dev/null 2>&1 || gcloud pubsub topics create "$TOPICO"
gcloud pubsub topics describe "$TOPICO_DLQ" >/dev/null 2>&1 || gcloud pubsub topics create "$TOPICO_DLQ"
gcloud pubsub topics add-iam-policy-binding "$TOPICO_DLQ" \
  --member="serviceAccount:${AGENTE_PUBSUB}" --role=roles/pubsub.publisher >/dev/null

passo "5/8 Construindo a imagem do pipeline (Cloud Build -> Artifact Registry)"
gcloud artifacts repositories describe "$REPO" --location="$REGIAO" >/dev/null 2>&1 \
  || gcloud artifacts repositories create "$REPO" --repository-format=docker --location="$REGIAO"
gcloud builds submit pipeline --tag "$IMAGEM"

passo "6/8 Serviço de limpeza no Cloud Run"
gcloud run deploy "$SERVICO" --image "$IMAGEM" --region "$REGIAO" \
  --service-account "$SA_LIMPEZA" --no-allow-unauthenticated \
  --set-env-vars "PROJETO=${PROJETO},DATASET=${DATASET}" \
  --concurrency 16 --max-instances 20 --memory 512Mi --cpu 1
URL=$(gcloud run services describe "$SERVICO" --region "$REGIAO" --format='value(status.url)')
gcloud run services add-iam-policy-binding "$SERVICO" --region "$REGIAO" \
  --member="serviceAccount:${SA_PUSH}" --role=roles/run.invoker >/dev/null

passo "7/8 Assinaturas: push para a limpeza, BigQuery para a bronze crua, inspeção da DLQ"
gcloud pubsub subscriptions describe "$SUB_PUSH" >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create "$SUB_PUSH" --topic="$TOPICO" \
    --push-endpoint="${URL}/pubsub" --push-auth-service-account="$SA_PUSH" \
    --ack-deadline=60 --dead-letter-topic="$TOPICO_DLQ" --max-delivery-attempts=5 \
    --min-retry-delay=5s --max-retry-delay=60s
gcloud pubsub subscriptions add-iam-policy-binding "$SUB_PUSH" \
  --member="serviceAccount:${AGENTE_PUBSUB}" --role=roles/pubsub.subscriber >/dev/null
gcloud pubsub subscriptions describe "$SUB_BQ" >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create "$SUB_BQ" --topic="$TOPICO" \
    --bigquery-table="${PROJETO}:${DATASET}.bronze_stream_eventos" --write-metadata
gcloud pubsub subscriptions describe "$SUB_DLQ" >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create "$SUB_DLQ" --topic="$TOPICO_DLQ"

passo "8/8 Job da gold no Cloud Run Jobs (ciclo a cada 30 s durante 55 min, com predição)"
gcloud run jobs deploy "$JOB_GOLD" --image "$IMAGEM" --region "$REGIAO" \
  --service-account "$SA_GOLD" --command python --args construir_gold.py \
  --set-env-vars "PROJETO=${PROJETO},DATASET=${DATASET},MODELO=${MODELO},ATRASO_SEGUNDOS=20,LOOP_SEGUNDOS=30,DURACAO_SEGUNDOS=3300" \
  --task-timeout 3600 --max-retries 0 --memory 512Mi

echo
echo "Pronto. Serviço de limpeza: ${URL}"
echo "Para iniciar a consolidação da gold:  gcloud run jobs execute ${JOB_GOLD} --region ${REGIAO}"
echo "Para enviar os dados:                 python produtor/produtor.py --projeto ${PROJETO} --topico ${TOPICO} --dados dados --taxa 200"
