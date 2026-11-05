#!/usr/bin/env bash
# =============================================================================
# Implanta a API REST de predição (api/) no Cloud Run.
# Rode no Cloud Shell, a partir da pasta raiz do projeto, DEPOIS de infra/registrar_modelo.sh:
#
#   bash infra/implantar_api.sh
#
# Variáveis úteis (todas opcionais):
#   BACKEND=savedmodel   (padrão) a API carrega o artefato do Cloud Storage e prevê em memória
#   BACKEND=vertex VERTEX_ENDPOINT_ID=123   a API consulta o endpoint criado por infra/endpoint_vertex.sh
#   PUBLICA=nao          exige autenticação (o padrão é aberta, para a demonstração com curl e /docs)
#   MIN_INSTANCIAS=1     mantém uma instância sempre ligada (sem partida a frio; tem custo enquanto ligada)
# =============================================================================
set -euo pipefail

DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAIZ="$(dirname "$DIR_SCRIPT")"
# Valores gravados pelo registrar_modelo.sh (projeto, região, bucket, versão e URI do artefato)
if [ -f "$DIR_SCRIPT/modelo_registrado.env" ]; then
  # shellcheck disable=SC1091
  set -a; source "$DIR_SCRIPT/modelo_registrado.env"; set +a
fi

PROJETO="${PROJETO:-platinum-avenue-505512-t5}"
REGIAO="${REGIAO:-us-central1}"
DATASET="${DATASET:-trabalho_pdm}"
VERSAO="${VERSAO:-v1}"
BUCKET="${BUCKET:-${PROJETO}-modelos-ideb}"
MODELO_URI="${MODELO_URI:-gs://${BUCKET}/modelo_ideb/${VERSAO}}"
BACKEND="${BACKEND:-savedmodel}"
VERTEX_ENDPOINT_ID="${VERTEX_ENDPOINT_ID:-}"
PUBLICA="${PUBLICA:-sim}"
MIN_INSTANCIAS="${MIN_INSTANCIAS:-0}"

SERVICO="api-ideb"
REPO="pdm-trabalho2"
IMAGEM="${REGIAO}-docker.pkg.dev/${PROJETO}/${REPO}/api-ideb:${VERSAO}"
SA_API="sa-api-ideb@${PROJETO}.iam.gserviceaccount.com"

passo() { echo; echo "==> $*"; }
gcloud config set project "$PROJETO" >/dev/null

passo "1/5 Habilitando APIs"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  aiplatform.googleapis.com storage.googleapis.com bigquery.googleapis.com

passo "2/5 Conta de serviço da API e permissões mínimas"
gcloud iam service-accounts describe "$SA_API" >/dev/null 2>&1 \
  || { gcloud iam service-accounts create sa-api-ideb --display-name="API de predicao do IDEB"; sleep 10; }
# ler o artefato do modelo
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${SA_API}" --role=roles/storage.objectViewer >/dev/null
# ler a gold (rota /escolas/{id}/predicao) e consultar o endpoint do Vertex AI (BACKEND=vertex)
for PAPEL in roles/bigquery.dataViewer roles/bigquery.jobUser roles/aiplatform.user; do
  gcloud projects add-iam-policy-binding "$PROJETO" --member="serviceAccount:${SA_API}" \
    --role="$PAPEL" --condition=None >/dev/null
done

passo "3/5 Construindo a imagem da API (Cloud Build -> Artifact Registry)"
gcloud artifacts repositories describe "$REPO" --location="$REGIAO" >/dev/null 2>&1 \
  || gcloud artifacts repositories create "$REPO" --repository-format=docker --location="$REGIAO"
gcloud builds submit "$RAIZ/api" --tag "$IMAGEM"

passo "4/5 Implantando ${SERVICO} no Cloud Run (backend ${BACKEND})"
ACESSO="--allow-unauthenticated"
[ "$PUBLICA" = "nao" ] && ACESSO="--no-allow-unauthenticated"
gcloud run deploy "$SERVICO" --image "$IMAGEM" --region "$REGIAO" \
  --service-account "$SA_API" $ACESSO \
  --set-env-vars "BACKEND=${BACKEND},MODELO_URI=${MODELO_URI},PROJETO=${PROJETO},REGIAO=${REGIAO},DATASET=${DATASET},VERTEX_ENDPOINT_ID=${VERTEX_ENDPOINT_ID}" \
  --memory 2Gi --cpu 1 --concurrency 20 --min-instances "$MIN_INSTANCIAS" --max-instances 5 \
  --timeout 60 --cpu-boost --port 8080
URL=$(gcloud run services describe "$SERVICO" --region "$REGIAO" --format='value(status.url)')

passo "5/5 Teste rápido"
CAB=()
[ "$PUBLICA" = "nao" ] && CAB=(-H "Authorization: Bearer $(gcloud auth print-identity-token)")
for _ in 1 2 3 4 5 6; do
  CODIGO=$(curl -s -o /tmp/saude.json -w '%{http_code}' "${CAB[@]}" "${URL}/saude" || true)
  [ "$CODIGO" = "200" ] && break
  echo "aguardando o modelo carregar (HTTP ${CODIGO})..."; sleep 10
done
cat /tmp/saude.json; echo
curl -s "${CAB[@]}" -X POST "${URL}/predicao" -H "Content-Type: application/json" \
  -d @"$RAIZ/api/exemplos/uma_escola.json" | python3 -m json.tool

touch "$DIR_SCRIPT/modelo_registrado.env"
sed -i '/^URL_API=/d' "$DIR_SCRIPT/modelo_registrado.env"
echo "URL_API=${URL}" >> "$DIR_SCRIPT/modelo_registrado.env"
echo
echo "Pronto. API: ${URL}"
echo "Documentação interativa: ${URL}/docs"
echo "Validação completa:  python api/cliente_validacao.py --url ${URL} --fonte lote --n 300"
