"""Modelo de teste no formato de um SavedModel exportado do BigQuery ML, para testar a API sem GCP.

Ele é construído com os PESOS REAIS do modelo_ideb (semente/pesos_modelo.csv, saída do ML.WEIGHTS)
e as estatísticas reais de treino (semente/features_modelo.csv, saída do ML.FEATURE_INFO):

    previsão = intercepto + soma(peso * valor) das numéricas + peso da categoria de cada texto

que é exatamente a regressão linear do BigQuery ML (os pesos de ML.WEIGHTS sem padronização se
aplicam aos valores originais). Categoria não vista no treino contribui com zero.

Como não sabemos de antemão todos os detalhes da assinatura que o EXPORT MODEL gera, o teste
constrói três variantes e a API precisa funcionar com todas:
  features   uma entrada por feature: int64, float64 e string, formato [None]
  coluna32   uma entrada por feature, numéricas em float32 e formato [None, 1]
  tf_example uma única entrada string com tf.Example serializado

Também gera o metadados_modelo.json com o mesmo script usado na nuvem (modelo/montar_metadados.py),
a partir de JSONs no formato que o `bq query --format=json` devolve.
"""
from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path

import numpy as np

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "api"))
sys.path.insert(0, str(RAIZ / "modelo"))
from app.features import FEATURES, NOMES  # noqa: E402
import montar_metadados  # noqa: E402

csv.field_size_limit(10**9)


def ler_pesos() -> tuple[float, dict[str, float], dict[str, dict[str, float]]]:
    numericos, categoricos, intercepto = {}, {}, 0.0
    with open(RAIZ / "semente" / "pesos_modelo.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            nome = r["processed_input"]
            cats = json.loads(r["category_weights"])["category_weights"]
            if nome == "__INTERCEPT__":
                intercepto = float(r["weight"])
            elif cats:
                categoricos[nome] = {c["category"]: float(c["weight"]) for c in cats}
            else:
                numericos[nome] = float(r["weight"])
    return intercepto, numericos, categoricos


def ler_feature_info() -> list[dict]:
    with open(RAIZ / "semente" / "features_modelo.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def medias_treino() -> dict[str, float]:
    return {r["input"]: float(r["mean"]) for r in ler_feature_info() if r["mean"] not in ("", None)}


# ---------------------------------------------------------------------------
# Referência em numpy (o "ML.PREDICT" do teste)
# ---------------------------------------------------------------------------
class Referencia:
    def __init__(self):
        self.b0, self.w, self.cat = ler_pesos()
        self.media = medias_treino()

    def prever(self, linha: dict) -> float:
        total = self.b0
        for nome, peso in self.w.items():
            v = linha.get(nome)
            if v is None or (isinstance(v, float) and math.isnan(v)) or v == "":
                v = self.media[nome]
            total += peso * float(v)
        for nome, tabela in self.cat.items():
            v = linha.get(nome)
            total += tabela.get("" if v is None else str(v), 0.0)
        return total


# ---------------------------------------------------------------------------
# SavedModel de teste
# ---------------------------------------------------------------------------
def construir_savedmodel(destino: Path, variante: str = "features") -> Path:
    import tensorflow as tf

    b0, w, cat = ler_pesos()
    tipos = dict(FEATURES)

    class Linear(tf.Module):
        def __init__(self):
            super().__init__()
            self.tabelas = {}
            for nome, tabela in cat.items():
                chaves = tf.constant(list(tabela.keys()), dtype=tf.string)
                valores = tf.constant(list(tabela.values()), dtype=tf.float64)
                self.tabelas[nome] = tf.lookup.StaticHashTable(
                    tf.lookup.KeyValueTensorInitializer(chaves, valores), default_value=tf.constant(0.0, tf.float64))

        def calcular(self, entradas: dict):
            total = tf.zeros_like(tf.cast(tf.reshape(entradas["ano"], [-1]), tf.float64)) + b0
            for nome, peso in w.items():
                total += peso * tf.cast(tf.reshape(entradas[nome], [-1]), tf.float64)
            for nome, tab in self.tabelas.items():
                total += tab.lookup(tf.reshape(entradas[nome], [-1]))
            return {"predicted_ideb": total}

    m = Linear()
    if variante == "tf_example":
        spec_ex = {n: tf.io.FixedLenFeature([], tf.string if t == "STRING" else
                                            (tf.int64 if t == "INT64" else tf.float32)) for n, t in FEATURES}

        @tf.function(input_signature=[tf.TensorSpec([None], tf.string, name="examples")])
        def servir(examples):
            return m.calcular(tf.io.parse_example(examples, spec_ex))
        assinatura = servir.get_concrete_function()
    else:
        def spec(nome):
            t = tipos[nome]
            if variante == "coluna32":
                dtype = tf.string if t == "STRING" else tf.float32
                return tf.TensorSpec([None, 1], dtype, name=nome)
            dtype = tf.string if t == "STRING" else (tf.int64 if t == "INT64" else tf.float64)
            return tf.TensorSpec([None], dtype, name=nome)

        @tf.function
        def servir(entradas):
            return m.calcular(entradas)
        assinatura = servir.get_concrete_function({n: spec(n) for n in NOMES})

    dir_modelo = destino / "modelo"
    tf.saved_model.save(m, str(dir_modelo), signatures={"serving_default": assinatura})
    return dir_modelo


def gravar_metadados(destino: Path, uri_artefato: str = "teste-local") -> Path:
    """Gera o metadados_modelo.json com o script de produção, a partir de JSONs no formato do bq."""
    fi = [{k: (v if v != "" else None) for k, v in r.items()} for r in ler_feature_info()]
    pesos = []
    with open(RAIZ / "semente" / "pesos_modelo.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            cats = json.loads(r["category_weights"])["category_weights"]
            pesos.append({"processed_input": r["processed_input"], "weight": r["weight"] or None,
                          "categorias": sorted(c["category"] for c in cats)})
    avaliacao = [{"mean_absolute_error": "0.467", "mean_squared_error": "0.358", "r2_score": "0.532",
                  "median_absolute_error": "0.386", "mean_squared_log_error": "0.0152"}]
    meta = montar_metadados.montar(fi, pesos, avaliacao, modelo="modelo_ideb", versao="v1",
                                   projeto="teste", dataset="teste", uri_artefato=uri_artefato)
    caminho = destino / "metadados_modelo.json"
    caminho.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return caminho


def construir(destino: Path, variante: str = "features") -> Path:
    destino.mkdir(parents=True, exist_ok=True)
    construir_savedmodel(destino, variante)
    gravar_metadados(destino)
    return destino


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Gera um modelo de teste no formato do EXPORT MODEL.")
    ap.add_argument("--saida", default="modelo_teste")
    ap.add_argument("--variante", default="features", choices=["features", "coluna32", "tf_example"])
    a = ap.parse_args()
    print("modelo de teste em", construir(Path(a.saida), a.variante))
