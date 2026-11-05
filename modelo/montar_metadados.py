"""Monta o metadados_modelo.json que acompanha o artefato exportado do modelo_ideb.

O SavedModel exportado pelo BigQuery ML guarda os pesos e o pré-processamento, mas não guarda
três informações de que a API precisa:

  * a média de treino de cada feature numérica, usada para preencher valores nulos exatamente
    como o ML.PREDICT faz (o BigQuery ML substitui NULL numérico pela média do treino);
  * as categorias vistas no treino de cada feature de texto, para avisar quando chega uma
    categoria nova (ela não tem peso no modelo e contribui com zero);
  * as métricas da avaliação oficial da fase 1 (ano de 2019), que a rota /modelo exibe.

Entradas: as saídas em JSON de três consultas feitas pelo infra/registrar_modelo.sh com
`bq query --format=json` (ML.FEATURE_INFO, ML.WEIGHTS e ML.EVALUATE). Só usa a biblioteca padrão.

Uso:
  python3 modelo/montar_metadados.py --feature-info fi.json --pesos pesos.json --avaliacao aval.json \
      --modelo modelo_ideb --versao v1 --projeto P --dataset D --uri-artefato gs://.../v1/modelo \
      --saida metadados_modelo.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path


def _num(v):
    """O bq CLI devolve números como texto em --format=json; converte e trata vazios."""
    if v is None or v == "" or v == "NULL":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _ler(caminho: str):
    """Lê a saída do bq. Ignora qualquer texto antes do JSON (avisos que o bq às vezes imprime)."""
    texto = Path(caminho).read_text(encoding="utf-8")
    inicio = min([i for i in (texto.find("["), texto.find("{")) if i >= 0], default=-1)
    return json.loads(texto[inicio:]) if inicio >= 0 else []


def montar(feature_info: list[dict], pesos: list[dict], avaliacao: list[dict], **info) -> dict:
    categorias = {}
    peso_numerico = {}
    intercepto = None
    for linha in pesos:
        nome = linha.get("processed_input")
        cats = linha.get("categorias") or []
        if nome == "__INTERCEPT__":
            intercepto = _num(linha.get("weight"))
        elif cats:
            categorias[nome] = sorted(str(c) for c in cats)
        else:
            peso_numerico[nome] = _num(linha.get("weight"))

    features = []
    for linha in feature_info:
        nome = linha["input"]
        if _num(linha.get("category_count")) is not None or nome in categorias:
            features.append({
                "nome": nome,
                "tipo": "categorica",
                "categorias_no_treino": categorias.get(nome, []),
                "nulos_no_treino": int(_num(linha.get("null_count")) or 0),
            })
        else:
            features.append({
                "nome": nome,
                "tipo": "numerica",
                "media_treino": _num(linha.get("mean")),
                "min_treino": _num(linha.get("min")),
                "max_treino": _num(linha.get("max")),
                "desvio_treino": _num(linha.get("stddev")),
                "nulos_no_treino": int(_num(linha.get("null_count")) or 0),
                "peso": peso_numerico.get(nome),
            })

    metricas = {}
    if avaliacao:
        for chave, valor in avaliacao[0].items():
            if _num(valor) is not None:
                metricas[chave] = round(_num(valor), 6)

    return {
        "modelo": info["modelo"],
        "versao": info["versao"],
        "tipo": "BigQuery ML linear_reg (exportado como TensorFlow SavedModel)",
        "alvo": "ideb",
        "projeto": info["projeto"],
        "dataset": info["dataset"],
        "uri_artefato": info["uri_artefato"],
        "exportado_em": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "treino": "gold_ideb_features com ano < 2019 (2013 e 2015)",
        "avaliacao_oficial": {"conjunto": "gold_ideb_features, ano = 2019", **metricas},
        "intercepto": intercepto,
        "quantidade_features": len(features),
        "features": features,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--feature-info", required=True)
    ap.add_argument("--pesos", required=True)
    ap.add_argument("--avaliacao", required=True)
    ap.add_argument("--modelo", default="modelo_ideb")
    ap.add_argument("--versao", default="v1")
    ap.add_argument("--projeto", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--uri-artefato", required=True)
    ap.add_argument("--saida", required=True)
    a = ap.parse_args()

    meta = montar(_ler(a.feature_info), _ler(a.pesos), _ler(a.avaliacao), modelo=a.modelo, versao=a.versao,
                  projeto=a.projeto, dataset=a.dataset, uri_artefato=a.uri_artefato)
    Path(a.saida).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    n_cat = sum(1 for f in meta["features"] if f["tipo"] == "categorica")
    print(f"metadados gravados em {a.saida}: {meta['quantidade_features']} features "
          f"({n_cat} categóricas), métricas {sorted(meta['avaliacao_oficial'])}")


if __name__ == "__main__":
    main()
