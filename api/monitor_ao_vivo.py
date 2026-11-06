"""Painel ao vivo para a demonstração: o streaming chegando e a API prevendo sozinha.

A cada poucos segundos o script:
  1. conta as mensagens que chegaram na bronze de streaming no último minuto;
  2. busca as escolas que o job acabou de consolidar na gold de streaming (com a predição que o
     próprio job gravou via ML.PREDICT);
  3. envia essas escolas à API no Cloud Run (POST /predicao) e mostra, lado a lado, o IDEB real,
     a previsão da API, a do BigQuery e a diferença.

Nada precisa ser clicado: basta deixar rodando numa aba do Cloud Shell enquanto o produtor publica.

Uso (Cloud Shell, na raiz do repositório):
  pip install -r api/requirements-cliente.txt
  python api/monitor_ao_vivo.py --url "$URL_API"
  python api/monitor_ao_vivo.py --url "$URL_API" --desde-minutos 10   # inclui o que chegou antes

Ctrl+C encerra e mostra o resumo.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import statistics
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cliente_validacao import EXTRAS_STREAM, instancia  # noqa: E402

VERDE, AMARELO, CINZA, NEGRITO, FIM = "\033[32m", "\033[33m", "\033[90m", "\033[1m", "\033[0m"


def consultar(cliente, projeto: str, dataset: str, desde: dt.datetime, ja_vistas: set) -> tuple[int, int, list[dict]]:
    """Devolve (mensagens no último minuto, escolas na gold de streaming, escolas novas)."""
    from google.cloud import bigquery

    t = lambda nome: f"`{projeto}.{dataset}.{nome}`"  # noqa: E731
    contagem = cliente.query(f"""
        SELECT
          (SELECT COUNT(*) FROM {t('bronze_stream_eventos')}
            WHERE DATE(publish_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
              AND publish_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 SECOND)) AS msgs_min,
          (SELECT COUNT(*) FROM {t('gold_ideb_features_stream')}) AS escolas_gold""").result()
    linha = next(iter(contagem))

    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("desde", "TIMESTAMP", desde)])
    novas = cliente.query(f"""
        SELECT g.* EXCEPT ({EXTRAS_STREAM}), g.versao, g.atualizado_em, p.ideb_previsto AS previsto_bq
        FROM {t('gold_ideb_features_stream')} AS g
        JOIN {t('predicoes_ideb_stream')} AS p
          ON p.id_escola = g.id_escola AND p.ano = g.ano AND p.versao_gold = g.versao
        WHERE DATE(g.primeira_carga_em) >= DATE_SUB(DATE(@desde), INTERVAL 1 DAY)
          AND g.atualizado_em >= @desde
        ORDER BY g.atualizado_em
        LIMIT 500""", job_config=cfg).result()
    escolas = [dict(r.items()) for r in novas]
    escolas = [e for e in escolas if (e["id_escola"], e["versao"]) not in ja_vistas]
    return int(linha["msgs_min"] or 0), int(linha["escolas_gold"] or 0), escolas


def prever(sessao: requests.Session, url: str, escolas: list[dict]) -> tuple[list[float], float, str]:
    """Uma chamada POST /predicao para todas as escolas novas (até 1.000 por chamada)."""
    corpo = {"instancias": [instancia(e) for e in escolas]}
    inicio = time.perf_counter()
    r = sessao.post(f"{url}/predicao", json=corpo, timeout=60)
    ms = (time.perf_counter() - inicio) * 1000
    r.raise_for_status()
    dados = r.json()
    return [p["ideb_previsto"] for p in dados["predicoes"]], ms, dados["modelo"]["versao"]


def desenhar(linhas: list[dict], msgs_min: int, escolas_gold: int, total: int, iguais: int,
             latencias: list[float], versao: str, url: str, max_linhas: int) -> None:
    print("\033[2J\033[H", end="")  # limpa a tela
    agora = dt.datetime.now().strftime("%H:%M:%S")
    lat = f"{statistics.median(latencias):.0f} ms" if latencias else "-"
    print(f"{NEGRITO}IDEB ao vivo · streaming -> gold -> API no Cloud Run{FIM}   {CINZA}{agora}{FIM}")
    print(f"{CINZA}{url}  ·  modelo {versao}{FIM}\n")
    print(f"  mensagens no último minuto  {NEGRITO}{msgs_min:>7,}{FIM}".replace(",", "."))
    print(f"  escolas na gold streaming   {NEGRITO}{escolas_gold:>7,}{FIM}".replace(",", "."))
    print(f"  previstas pela API          {NEGRITO}{total:>7,}{FIM}".replace(",", ".")
          + f"   iguais ao BigQuery {VERDE}{iguais}/{total}{FIM}   latência por chamada {NEGRITO}{lat}{FIM}\n")
    print(f"  {'hora':8}  {'escola':8}  {'UF':2}  {'v':>1}  {'IDEB real':>9}  {'API':>7}  {'BigQuery':>8}  {'diferença':>9}")
    print(f"  {'-' * 8}  {'-' * 8}  {'-' * 2}  {'-'}  {'-' * 9}  {'-' * 7}  {'-' * 8}  {'-' * 9}")
    for e in linhas[-max_linhas:]:
        cor = VERDE if e["dif"] < 1e-3 else AMARELO
        real = "-" if e.get("ideb") is None else f"{e['ideb']:.1f}"
        print(f"  {e['hora']:8}  {e['id_escola']:8}  {e.get('sigla_uf') or '':2}  {e['versao']:>1}  {real:>9}  "
              f"{e['previsto_api']:>7.3f}  {e['previsto_bq']:>8.3f}  {cor}{e['dif']:>9.1e}{FIM}")
    if not linhas:
        print(f"\n  {CINZA}aguardando escolas novas na gold (o job consolida a cada 30 s)...{FIM}")
    print(f"\n{CINZA}Ctrl+C para encerrar{FIM}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("URL_API"), help="URL da API (padrão: $URL_API)")
    ap.add_argument("--projeto", default="platinum-avenue-505512-t5")
    ap.add_argument("--dataset", default="trabalho_pdm")
    ap.add_argument("--intervalo", type=float, default=3.0, help="segundos entre consultas")
    ap.add_argument("--desde-minutos", type=float, default=0.0,
                    help="mostra também as escolas consolidadas nos últimos N minutos (padrão: só as novas)")
    ap.add_argument("--max-linhas", type=int, default=15, help="linhas da tabela na tela")
    a = ap.parse_args()
    if not a.url:
        ap.error("informe --url ou defina URL_API")

    from google.cloud import bigquery

    url = a.url.rstrip("/")
    sessao = requests.Session()
    cliente = bigquery.Client(project=a.projeto)
    versao = sessao.get(f"{url}/saude", timeout=120).json().get("modelo", {}).get("versao", "?")

    desde = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=a.desde_minutos)
    vistas: set = set()
    linhas: list[dict] = []
    latencias: list[float] = []
    iguais = 0
    try:
        while True:
            msgs_min, escolas_gold, novas = consultar(cliente, a.projeto, a.dataset, desde, vistas)
            if novas:
                previstos, ms, versao = prever(sessao, url, novas)
                latencias.append(ms)
                hora = dt.datetime.now().strftime("%H:%M:%S")
                for e, p in zip(novas, previstos):
                    e.update(previsto_api=p, previsto_bq=float(e["previsto_bq"]), hora=hora,
                             dif=abs(p - float(e["previsto_bq"])))
                    iguais += e["dif"] < 1e-3
                    vistas.add((e["id_escola"], e["versao"]))
                linhas.extend(novas)
            desenhar(linhas, msgs_min, escolas_gold, len(linhas), iguais, latencias, versao, url, a.max_linhas)
            time.sleep(a.intervalo)
    except KeyboardInterrupt:
        print(f"\n{len(linhas)} escolas previstas pela API, {iguais} iguais ao BigQuery ML (diferença < 0,001).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
