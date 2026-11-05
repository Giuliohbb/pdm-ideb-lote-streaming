#!/usr/bin/env bash
# =============================================================================
# OPCIONAL. Implanta a versão registrada do modelo num Endpoint do Vertex AI (predição online
# gerenciada pelo Google), para mostrar a segunda forma de servir o modelo.
#
#   bash infra/endpoint_vertex.sh implantar   cria o endpoint e implanta o modelo (leva de 10 a 20 minutos)
#   bash infra/endpoint_vertex.sh status      mostra o que está implantado
#   bash infra/endpoint_vertex.sh remover     desimplanta e apaga o endpoint
#
# ATENÇÃO AO CUSTO: o endpoint cobra por hora de nó enquanto o modelo estiver implantado, mesmo
# sem nenhuma requisição. Implante no dia da apresentação e rode "remover" logo depois.
# A API não depende disto: no padrão (BACKEND=savedmodel) ela carrega o artefato sozinha.
# =============================================================================
set -euo pipefail
DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAIZ="$(dirname "$DIR_SCRIPT")"
if [ -f "$DIR_SCRIPT/modelo_registrado.env" ]; then
  # shellcheck disable=SC1091
  set -a; source "$DIR_SCRIPT/modelo_registrado.env"; set +a
fi
PROJETO="${PROJETO:-platinum-avenue-505512-t5}"
REGIAO="${REGIAO:-us-central1}"
VERSAO="${VERSAO:-v1}"
NOME_VERTEX="${NOME_VERTEX:-modelo-ideb}"
NOME_ENDPOINT="${NOME_ENDPOINT:-endpoint-ideb}"
MAQUINA="${MAQUINA:-n1-standard-2}"
gcloud config set project "$PROJETO" >/dev/null

endpoint_id() {
  gcloud ai endpoints list --region="$REGIAO" --filter="displayName=${NOME_ENDPOINT}" \
    --format="value(name)" 2>/dev/null | head -1 | awk -F/ '{print $NF}'
}

case "${1:-}" in
  implantar)
    MODELO_ID=$(gcloud ai models list --region="$REGIAO" --filter="displayName=${NOME_VERTEX}" \
      --format="value(name)" | head -1 | awk -F/ '{print $NF}')
    [ -z "$MODELO_ID" ] && { echo "modelo ${NOME_VERTEX} não registrado; rode infra/registrar_modelo.sh"; exit 1; }
    EID=$(endpoint_id)
    if [ -z "$EID" ]; then
      gcloud ai endpoints create --region="$REGIAO" --display-name="$NOME_ENDPOINT"
      EID=$(endpoint_id)
    fi
    echo "endpoint ${EID}; implantando o modelo ${MODELO_ID} (versão padrão) em ${MAQUINA}..."
    gcloud ai endpoints deploy-model "$EID" --region="$REGIAO" --model="$MODELO_ID" \
      --display-name="${NOME_VERTEX}-${VERSAO}" --machine-type="$MAQUINA" \
      --min-replica-count=1 --max-replica-count=1 --traffic-split=0=100
    echo "Teste direto no endpoint:"
    gcloud ai endpoints predict "$EID" --region="$REGIAO" --json-request="$RAIZ/api/exemplos/vertex_uma_escola.json"
    touch "$DIR_SCRIPT/modelo_registrado.env"
    sed -i '/^VERTEX_ENDPOINT_ID=/d' "$DIR_SCRIPT/modelo_registrado.env"
    echo "VERTEX_ENDPOINT_ID=${EID}" >> "$DIR_SCRIPT/modelo_registrado.env"
    echo
    echo "Para a API passar a consultar o endpoint:"
    echo "  BACKEND=vertex VERTEX_ENDPOINT_ID=${EID} bash infra/implantar_api.sh"
    ;;
  status)
    EID=$(endpoint_id)
    [ -z "$EID" ] && { echo "nenhum endpoint ${NOME_ENDPOINT}"; exit 0; }
    gcloud ai endpoints describe "$EID" --region="$REGIAO" \
      --format="yaml(displayName, deployedModels[].id, deployedModels[].model, deployedModels[].dedicatedResources.machineSpec)"
    ;;
  remover)
    EID=$(endpoint_id)
    [ -z "$EID" ] && { echo "nenhum endpoint ${NOME_ENDPOINT}"; exit 0; }
    for DM in $(gcloud ai endpoints describe "$EID" --region="$REGIAO" --format="value(deployedModels.id)" | tr ';,' '  '); do
      gcloud ai endpoints undeploy-model "$EID" --region="$REGIAO" --deployed-model-id="$DM"
    done
    gcloud ai endpoints delete "$EID" --region="$REGIAO" --quiet
    [ -f "$DIR_SCRIPT/modelo_registrado.env" ] && sed -i '/^VERTEX_ENDPOINT_ID=/d' "$DIR_SCRIPT/modelo_registrado.env"
    echo "Endpoint removido. Se a API estava com BACKEND=vertex, reimplante com: bash infra/implantar_api.sh"
    ;;
  *)
    echo "uso: $0 implantar|status|remover"; exit 1 ;;
esac
