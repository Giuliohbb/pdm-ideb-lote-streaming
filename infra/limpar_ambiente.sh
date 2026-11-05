#!/usr/bin/env bash
# =============================================================================
# Utilidades para depois da demonstração.
#
#   bash infra/limpar_ambiente.sh zerar     apaga só os DADOS de streaming (mantém a infraestrutura),
#                                           útil para ensaiar a demo várias vezes
#   bash infra/limpar_ambiente.sh desligar  remove assinaturas, tópicos, o serviço de limpeza, o job da gold,
#                                           a API e o endpoint do Vertex AI (se existir).
#                                           Ficam: as tabelas do BigQuery, o artefato do modelo no
#                                           Cloud Storage e o registro no Vertex AI Model Registry,
#                                           que não custam nada parados e servem de evidência da entrega.
# =============================================================================
set -euo pipefail
PROJETO="${PROJETO:-platinum-avenue-505512-t5}"
REGIAO="${REGIAO:-us-central1}"
DATASET="${DATASET:-trabalho_pdm}"

case "${1:-}" in
  zerar)
    # DELETE/TRUNCATE não funcionam enquanto há linhas no buffer de streaming; DROP + CREATE sempre funciona.
    for T in bronze_stream_eventos silver_stream_ideb_escola silver_stream_censo_escola \
             silver_stream_indicadores_escola silver_stream_inse_escola silver_stream_censo_docente \
             auditoria_limpeza_stream gold_ideb_features_stream predicoes_ideb_stream; do
      echo "removendo ${T}"
      bq rm -f -t "${PROJETO}:${DATASET}.${T}"
    done
    bq query --project_id="$PROJETO" --use_legacy_sql=false "$(cat sql/01_tabelas_stream.sql)" >/dev/null
    bq query --project_id="$PROJETO" --use_legacy_sql=false "$(cat sql/02_views.sql)" >/dev/null
    echo "Tabelas recriadas vazias. Se a assinatura BigQuery reclamar, aguarde 1 a 2 minutos."
    ;;
  desligar)
    gcloud pubsub subscriptions delete sub-limpeza-push sub-bronze-bigquery sub-dlq-inspecao --quiet || true
    gcloud pubsub topics delete ideb-bronze-eventos ideb-bronze-dlq --quiet || true
    gcloud run services delete limpeza-ideb --region "$REGIAO" --quiet || true
    gcloud run jobs delete gold-ideb --region "$REGIAO" --quiet || true
    gcloud run services delete api-ideb --region "$REGIAO" --quiet || true
    bash "$(dirname "${BASH_SOURCE[0]}")/endpoint_vertex.sh" remover || true
    ;;
  *)
    echo "uso: $0 zerar|desligar"; exit 1 ;;
esac
