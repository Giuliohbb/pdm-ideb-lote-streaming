#!/usr/bin/env bash
# =============================================================================
# Exporta o modelo_ideb do BigQuery ML e o registra no Vertex AI Model Registry.
# Rode no Cloud Shell, a partir da pasta raiz do projeto:
#
#   bash infra/registrar_modelo.sh
#
# O que acontece, em ordem:
#   1. habilita as APIs do Vertex AI e do Cloud Storage;
#   2. descobre a localização do dataset e cria um bucket compatível com ela;
#   3. EXPORT MODEL: grava o modelo como TensorFlow SavedModel em gs://BUCKET/modelo_ideb/VERSAO/modelo/
#      (o artefato pronto para produção, com o pré-processamento do BigQuery ML embutido);
#   4. grava ao lado o metadados_modelo.json (médias de treino, categorias, métricas de 2019);
#   5. registra o artefato no Vertex AI Model Registry como uma versão do modelo "modelo-ideb";
#   6. grava infra/modelo_registrado.env, lido pelos scripts da API e do endpoint.
#
# É idempotente: se a versão já foi exportada ou registrada, o passo é pulado.
# Para publicar uma versão nova do modelo: VERSAO=v2 bash infra/registrar_modelo.sh
# =============================================================================
set -euo pipefail

PROJETO="${PROJETO:-platinum-avenue-505512-t5}"
REGIAO="${REGIAO:-us-central1}"
DATASET="${DATASET:-trabalho_pdm}"
MODELO="${MODELO:-modelo_ideb}"
VERSAO="${VERSAO:-v1}"
BUCKET="${BUCKET:-${PROJETO}-modelos-ideb}"
NOME_VERTEX="${NOME_VERTEX:-modelo-ideb}"

BASE_URI="gs://${BUCKET}/${MODELO}/${VERSAO}"
URI_MODELO="${BASE_URI}/modelo"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAIZ="$(dirname "$DIR_SCRIPT")"

passo() { echo; echo "==> $*"; }
bqj() { bq --quiet --headless --format=json "$@"; }

gcloud config set project "$PROJETO" >/dev/null

passo "1/6 Habilitando as APIs do Vertex AI e do Cloud Storage"
gcloud services enable aiplatform.googleapis.com storage.googleapis.com bigquery.googleapis.com

passo "2/6 Localização do dataset e bucket do modelo"
LOCAL_DATASET=$(bqj show "${PROJETO}:${DATASET}" \
  | python3 -c 'import json,sys; t=sys.stdin.read(); print(json.loads(t[t.index("{"):])["location"])')
case "$LOCAL_DATASET" in
  US|us)
    # Um bucket regional dentro da multirregião US é aceito pelo EXPORT MODEL
    # e fica na mesma região do Vertex AI.
    LOCAL_BUCKET="$REGIAO" ;;
  EU|eu)
    LOCAL_BUCKET="${REGIAO_EU:-europe-west4}"; REGIAO="$LOCAL_BUCKET" ;;
  *)
    LOCAL_BUCKET=$(echo "$LOCAL_DATASET" | tr '[:upper:]' '[:lower:]'); REGIAO="$LOCAL_BUCKET" ;;
esac
echo "dataset em ${LOCAL_DATASET}; bucket e Vertex AI em ${REGIAO}"
if ! gcloud storage buckets describe "gs://${BUCKET}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET}" --location="$LOCAL_BUCKET" --uniform-bucket-level-access
fi

passo "3/6 Exportando ${MODELO} para ${URI_MODELO}"
if gcloud storage ls "${URI_MODELO}/saved_model.pb" >/dev/null 2>&1; then
  echo "a versão ${VERSAO} já foi exportada; pulando (apague ${BASE_URI} para exportar de novo)"
else
  bq --quiet --headless query --use_legacy_sql=false \
    "EXPORT MODEL \`${PROJETO}.${DATASET}.${MODELO}\` OPTIONS (URI = '${URI_MODELO}/')"
fi
gcloud storage ls -r "${URI_MODELO}/" | head -20

passo "4/6 Metadados do modelo (ML.FEATURE_INFO, ML.WEIGHTS e a avaliação oficial de 2019)"
bqj query --use_legacy_sql=false --max_rows=1000 \
  "SELECT * FROM ML.FEATURE_INFO(MODEL \`${PROJETO}.${DATASET}.${MODELO}\`)" > "$TMP/feature_info.json"
bqj query --use_legacy_sql=false --max_rows=1000 \
  "SELECT processed_input, weight,
          ARRAY(SELECT category FROM UNNEST(category_weights) ORDER BY category) AS categorias
   FROM ML.WEIGHTS(MODEL \`${PROJETO}.${DATASET}.${MODELO}\`)" > "$TMP/pesos.json"
bqj query --use_legacy_sql=false \
  "SELECT * FROM ML.EVALUATE(MODEL \`${PROJETO}.${DATASET}.${MODELO}\`,
     (SELECT * EXCEPT (id_escola) FROM \`${PROJETO}.${DATASET}.gold_ideb_features\` WHERE ano = 2019))" \
  > "$TMP/avaliacao.json"
python3 "$RAIZ/modelo/montar_metadados.py" --feature-info "$TMP/feature_info.json" --pesos "$TMP/pesos.json" \
  --avaliacao "$TMP/avaliacao.json" --modelo "$MODELO" --versao "$VERSAO" --projeto "$PROJETO" \
  --dataset "$DATASET" --uri-artefato "$URI_MODELO" --saida "$TMP/metadados_modelo.json"
gcloud storage cp "$TMP/metadados_modelo.json" "${BASE_URI}/metadados_modelo.json"
R2=$(python3 -c "import json; print(json.load(open('$TMP/metadados_modelo.json'))['avaliacao_oficial'].get('r2_score','?'))")
MAE=$(python3 -c "import json; print(json.load(open('$TMP/metadados_modelo.json'))['avaliacao_oficial'].get('mean_absolute_error','?'))")

passo "5/6 Registrando no Vertex AI Model Registry (${NOME_VERTEX}, versão ${VERSAO})"
case "$REGIAO" in
  europe-*) PREFIXO="europe-docker.pkg.dev" ;;
  asia-*|australia-*|me-*) PREFIXO="asia-docker.pkg.dev" ;;
  *) PREFIXO="us-docker.pkg.dev" ;;
esac
CONTAINER="${CONTAINER_PREDICAO:-${PREFIXO}/vertex-ai/prediction/tf2-cpu.2-15:latest}"
EXISTENTE=$(gcloud ai models list --region="$REGIAO" --filter="displayName=${NOME_VERTEX}" \
  --format="value(name)" 2>/dev/null | head -1)
JA_TEM_VERSAO=""
if [ -n "$EXISTENTE" ]; then
  JA_TEM_VERSAO=$(gcloud ai models list-version "$EXISTENTE" --region="$REGIAO" \
    --format="value(versionAliases)" 2>/dev/null | tr ';,' '\n\n' | grep -x "$VERSAO" || true)
fi
if [ -n "$JA_TEM_VERSAO" ]; then
  echo "a versão ${VERSAO} já está registrada em ${EXISTENTE}; pulando"
else
  ARGS=(--region="$REGIAO" --display-name="$NOME_VERTEX" --artifact-uri="$URI_MODELO"
        --container-image-uri="$CONTAINER" --version-aliases="$VERSAO"
        --description="Regressao linear do IDEB (anos finais), BigQuery ML ${PROJETO}.${DATASET}.${MODELO}. Treino 2013 e 2015; avaliacao 2019: R2=${R2}, MAE=${MAE}."
        --labels="origem=bigquery-ml,dataset=${DATASET},versao=${VERSAO},trabalho=pdm-ufg")
  if [ -n "$EXISTENTE" ]; then ARGS+=(--parent-model="${EXISTENTE}")
    if [[ "$EXISTENTE" != projects/* ]]; then
      ARGS[${#ARGS[@]}-1]="--parent-model=projects/${PROJETO}/locations/${REGIAO}/models/${EXISTENTE}"
    fi; fi
  gcloud ai models upload "${ARGS[@]}"
  EXISTENTE=$(gcloud ai models list --region="$REGIAO" --filter="displayName=${NOME_VERTEX}" \
    --format="value(name)" | head -1)
fi
gcloud ai models list-version "$EXISTENTE" --region="$REGIAO" \
  --format="table(versionId, versionAliases.join(','), versionCreateTime)" || true

passo "6/6 Gravando infra/modelo_registrado.env"
cat > "$DIR_SCRIPT/modelo_registrado.env" <<EOF
# Gerado por infra/registrar_modelo.sh em $(date -u +%Y-%m-%dT%H:%M:%SZ)
PROJETO=${PROJETO}
REGIAO=${REGIAO}
DATASET=${DATASET}
BUCKET=${BUCKET}
VERSAO=${VERSAO}
MODELO_URI=${BASE_URI}
VERTEX_MODELO=${EXISTENTE}
EOF
cat "$DIR_SCRIPT/modelo_registrado.env"

echo
echo "Pronto. Modelo registrado: ${EXISTENTE}"
echo "Console: https://console.cloud.google.com/vertex-ai/models?project=${PROJETO}"
echo "Próximo passo: bash infra/implantar_api.sh"
