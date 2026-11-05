"""Validação da API em produção: a previsão servida é a mesma do BigQuery ML?

O script pega escolas da gold, envia cada uma à API (POST /predicao) e compara a resposta com a
predição que o próprio BigQuery calculou com ML.PREDICT. Se o artefato exportado e o pré-
processamento da API estiverem corretos, as duas coincidem (diferenças só de arredondamento).
Depois mede a latência com requisições de uma escola por vez.

Fontes de escolas:
  streaming  gold_ideb_features_stream + predicoes_ideb_stream (o que o job da gold gravou)
  lote       gold_ideb_features de 2019 + ML.PREDICT calculado na hora
  csv        um arquivo local com as colunas da gold e uma coluna com a predição esperada (testes)

Uso (Cloud Shell):
  pip install -r api/requirements-cliente.txt
  python api/cliente_validacao.py --url "$URL_API" --fonte streaming --n 300
  python api/cliente_validacao.py --url "$URL_API" --fonte lote --n 300

Termina com código 0 se todas as diferenças ficarem abaixo da tolerância e 1 caso contrário.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import subprocess
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from app.features import FEATURES, NOMES  # noqa: E402

TIPOS = dict(FEATURES)
EXTRAS_STREAM = "origem, primeira_carga_em, atualizado_em, versao, _hash"


# ---------------------------------------------------------------------------
# Fontes
# ---------------------------------------------------------------------------
def de_bigquery(fonte: str, projeto: str, dataset: str, modelo: str, n: int) -> list[dict]:
    from google.cloud import bigquery

    t = lambda nome: f"`{projeto}.{dataset}.{nome}`"  # noqa: E731
    if fonte == "streaming":
        sql = f"""
        SELECT g.* EXCEPT ({EXTRAS_STREAM}), p.ideb_previsto AS esperado
        FROM {t('gold_ideb_features_stream')} AS g
        JOIN {t('predicoes_ideb_stream')} AS p
          ON p.id_escola = g.id_escola AND p.ano = g.ano AND p.versao_gold = g.versao
        ORDER BY FARM_FINGERPRINT(g.id_escola)
        LIMIT {int(n)}"""
    else:
        sql = f"""
        SELECT * EXCEPT (predicted_ideb), predicted_ideb AS esperado
        FROM ML.PREDICT(MODEL {t(modelo)}, (
          SELECT * FROM {t('gold_ideb_features')} WHERE ano = 2019
          ORDER BY FARM_FINGERPRINT(id_escola) LIMIT {int(n)}))"""
    cliente = bigquery.Client(project=projeto)
    return [dict(r.items()) for r in cliente.query(sql).result()]


def de_csv(caminho: str, coluna_esperada: str, n: int) -> list[dict]:
    def conv(nome, v):
        if v in ("", None):
            return None
        t = TIPOS.get(nome)
        if t == "INT64":
            return int(float(v))
        if t == "FLOAT64" or nome in ("ideb", "esperado"):
            return float(v)
        return v
    with open(caminho, encoding="utf-8") as f:
        linhas = []
        for r in csv.DictReader(f):
            r["esperado"] = r.pop(coluna_esperada)
            linhas.append({k: conv(k, v) for k, v in r.items()})
            if len(linhas) >= n:
                break
    return linhas


def instancia(linha: dict) -> dict:
    d = {n: linha.get(n) for n in NOMES}
    d["id_escola"] = None if linha.get("id_escola") is None else str(linha["id_escola"])
    d["ideb"] = linha.get("ideb")
    return json.loads(json.dumps(d, default=str))


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", required=True, help="URL da API, por exemplo https://api-ideb-xxxx.run.app")
    ap.add_argument("--fonte", choices=["streaming", "lote", "csv"], default="streaming")
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--projeto", default="platinum-avenue-505512-t5")
    ap.add_argument("--dataset", default="trabalho_pdm")
    ap.add_argument("--modelo", default="modelo_ideb")
    ap.add_argument("--csv", help="arquivo para --fonte csv")
    ap.add_argument("--coluna-esperada", default="esperado")
    ap.add_argument("--tolerancia", type=float, default=1e-3)
    ap.add_argument("--lote", type=int, default=100, help="escolas por requisição na comparação")
    ap.add_argument("--latencia", type=int, default=30, help="requisições de 1 escola para medir latência")
    ap.add_argument("--privada", action="store_true", help="a API exige autenticação (usa gcloud)")
    a = ap.parse_args()

    url = a.url.rstrip("/")
    sessao = requests.Session()
    if a.privada:
        token = subprocess.run(["gcloud", "auth", "print-identity-token"], capture_output=True, text=True,
                               check=True).stdout.strip()
        sessao.headers["Authorization"] = f"Bearer {token}"

    r = sessao.get(f"{url}/saude", timeout=120)
    print(f"GET /saude -> {r.status_code} {r.text[:200]}")
    if r.status_code != 200:
        return 1

    if a.fonte == "csv":
        linhas = de_csv(a.csv, a.coluna_esperada, a.n)
    else:
        print(f"buscando {a.n} escolas da fonte '{a.fonte}' no BigQuery...")
        linhas = de_bigquery(a.fonte, a.projeto, a.dataset, a.modelo, a.n)
    if not linhas:
        print("nenhuma escola encontrada; a gold de streaming já foi preenchida pelo job?")
        return 1

    # 1) Concordância com o BigQuery ML
    api, avisos = [], 0
    for i in range(0, len(linhas), a.lote):
        corpo = {"instancias": [instancia(l) for l in linhas[i:i + a.lote]]}
        r = sessao.post(f"{url}/predicao", json=corpo, timeout=120)
        if r.status_code != 200:
            print(f"POST /predicao -> {r.status_code}: {r.text[:500]}")
            return 1
        for p in r.json()["predicoes"]:
            api.append(p["ideb_previsto"])
            avisos += bool(p["avisos"])
    difs = [abs(x - float(l["esperado"])) for x, l in zip(api, linhas)]
    dentro = sum(d <= a.tolerancia for d in difs)
    print(f"\nConcordância API x BigQuery ML ({a.fonte}): {len(difs)} escolas")
    print(f"  dentro da tolerância de {a.tolerancia}: {dentro} de {len(difs)}")
    print(f"  diferença máxima: {max(difs):.2e} · média: {statistics.fmean(difs):.2e}")
    print(f"  escolas com avisos (nulos ou categoria nova): {avisos}")
    reais = [(x, l["ideb"]) for x, l in zip(api, linhas) if l.get("ideb") is not None]
    if reais:
        mae = statistics.fmean(abs(x - y) for x, y in reais)
        print(f"  MAE da API contra o IDEB observado: {mae:.3f}")
    piores = sorted(zip(difs, linhas, api), key=lambda t: -t[0])[:3]
    for d, l, x in piores:
        print(f"    {l.get('id_escola')}: API {x:.6f} · BigQuery {float(l['esperado']):.6f} · dif {d:.2e}")

    # 2) Latência, uma escola por requisição
    tempos, servidor = [], []
    for l in linhas[:a.latencia]:
        t0 = time.perf_counter()
        r = sessao.post(f"{url}/predicao", json={"instancias": [instancia(l)]}, timeout=60)
        tempos.append((time.perf_counter() - t0) * 1000)
        if r.status_code == 200:
            servidor.append(r.json()["latencia_ms"])
    if tempos:
        q = lambda xs, p: sorted(xs)[min(len(xs) - 1, math.ceil(p * len(xs)) - 1)]  # noqa: E731
        print(f"\nLatência de ponta a ponta ({len(tempos)} requisições de 1 escola): "
              f"mediana {statistics.median(tempos):.0f} ms · p95 {q(tempos, 0.95):.0f} ms")
        if servidor:
            print(f"Tempo dentro da API: mediana {statistics.median(servidor):.1f} ms")

    # 3) Rota que lê a escola direto da gold
    if a.fonte != "csv":
        esc = linhas[0]["id_escola"]
        r = sessao.get(f"{url}/escolas/{esc}/predicao", timeout=60)
        print(f"\nGET /escolas/{esc}/predicao -> {r.status_code}")
        print(json.dumps(r.json(), ensure_ascii=False, indent=2)[:1200])

    ok = dentro == len(difs)
    print("\nRESULTADO:", "OK, a API serve o mesmo modelo do BigQuery ML" if ok else "DIVERGÊNCIA, ver acima")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
