#!/usr/bin/env bash
# =============================================================================
# Guarda as 5 bronzes do fluxo "em repouso" (registros reais de 2019 com ruído e defeitos controlados):
#   1. copia dados/bronze e dados/fluxo para um bucket do Cloud Storage
#      (o produtor pode ler direto de lá: --dados gs://BUCKET/dados);
#   2. carrega cada arquivo numa tabela bronze_fluxo_* do BigQuery, uma linha por registro,
#      com o JSON cru numa coluna STRING (do jeito que vai chegar ao Pub/Sub, sujeira incluída).
#
# Por que STRING e não colunas tipadas? Os arquivos têm, de propósito, tipos misturados
# ("4,7" e 4.7 na mesma coluna). Uma carga tipada rejeitaria justamente as linhas que o
# pipeline precisa limpar ao vivo.
#
#   bash infra/salvar_bronzes_fluxo.sh
# =============================================================================
set -euo pipefail
PROJETO="${PROJETO:-platinum-avenue-505512-t5}"
DATASET="${DATASET:-trabalho_pdm}"
BUCKET="${BUCKET:-${PROJETO}-pdm-trabalho2}"

gcloud storage buckets describe "gs://${BUCKET}" >/dev/null 2>&1 \
  || gcloud storage buckets create "gs://${BUCKET}" --location=US --project="$PROJETO"
gcloud storage cp -r dados/bronze dados/fluxo "gs://${BUCKET}/dados/"

for ARQ in bronze_ideb_escola bronze_censo_escola bronze_indicadores_escola bronze_inse_escola bronze_censo_docente; do
  echo "carregando ${ARQ}"
  bq load --project_id="$PROJETO" --replace --source_format=CSV --field_delimiter=tab --quote="" \
    "${DATASET}.${ARQ/bronze_/bronze_fluxo_}" "gs://${BUCKET}/dados/bronze/${ARQ}.jsonl" registro:STRING
done

echo
echo "Pronto. Exemplo de consulta às bronzes do fluxo:"
echo "  SELECT JSON_VALUE(registro, '\$.id_escola') AS id_escola, JSON_VALUE(registro, '\$.ideb') AS ideb_bruto"
echo "  FROM \`${PROJETO}.${DATASET}.bronze_fluxo_ideb_escola\` LIMIT 20"
