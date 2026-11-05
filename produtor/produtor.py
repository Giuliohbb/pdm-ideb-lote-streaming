"""Produtor de eventos: lê as 5 bronzes do fluxo (registros reais de 2019 com ruído e defeitos
controlados, gerados por gerador/gerar_bronzes_fluxo.py) e publica cada linha como uma mensagem JSON
num tópico do Pub/Sub, simulando o sistema de origem do INEP enviando registros em tempo real.

Cada mensagem leva o registro bruto (sujo) no corpo e estes atributos:
  tabela      ideb | censo_escola | indicadores | inse | docente  (roteia a limpeza)
  ref         <tabela>:<linha do arquivo>  (rastreia o registro até o gabarito)
  escola_seq  ordem da escola no lote
  evento_id   UUID do evento
  lote        identificador desta execução

Ordens de envio:
  escola     (padrão) escola a escola, como se cada escola enviasse seu censo; reproduz
             duplicatas tardias e dados de docente atrasados (ordem_eventos.csv do gerador)
  aleatoria  tudo embaralhado (chegada fora de ordem)
  tabela     uma tabela inteira depois da outra (parecido com cargas em lote)

Exemplos:
  python produtor.py --projeto P --topico ideb-bronze-eventos --dados ../dados --taxa 200
  python produtor.py --projeto P --topico ideb-bronze-eventos --dados ../dados --escolas 50 --taxa 20
  python produtor.py --dados ../dados --dry-run --escolas 3
  python produtor.py --projeto P --topico T --dados gs://meu-bucket/dados --taxa 0
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import tempfile
import time
import uuid
from pathlib import Path

ARQUIVOS = {
    "ideb": "bronze_ideb_escola.jsonl",
    "censo_escola": "bronze_censo_escola.jsonl",
    "indicadores": "bronze_indicadores_escola.jsonl",
    "inse": "bronze_inse_escola.jsonl",
    "docente": "bronze_censo_docente.jsonl",
}


def baixar_do_gcs(uri: str) -> Path:
    from google.cloud import storage
    bucket_nome, _, prefixo = uri[5:].partition("/")
    destino = Path(tempfile.mkdtemp(prefix="dados_ideb_"))
    cli = storage.Client()
    for blob in cli.list_blobs(bucket_nome, prefix=prefixo):
        if blob.name.endswith("/"):
            continue
        rel = blob.name[len(prefixo):].lstrip("/")
        alvo = destino / rel
        alvo.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(alvo)
    return destino


def carregar(dados: Path) -> tuple[dict[str, list[str]], list[dict], list[tuple]]:
    linhas = {t: (dados / "bronze" / a).read_text(encoding="utf-8").splitlines() for t, a in ARQUIVOS.items()}
    mal_path = dados / "bronze" / "mensagens_malformadas.jsonl"
    malformadas = [json.loads(x) for x in mal_path.read_text(encoding="utf-8").splitlines()] if mal_path.exists() else []
    ordem = []
    with open(dados / "fluxo" / "ordem_eventos.csv", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            ordem.append((r["tabela"], int(r["linha"]), int(r["escola_seq"]), r["tipo"]))
    return linhas, malformadas, ordem


def montar_eventos(linhas, malformadas, ordem, args) -> list[tuple[str, bytes, dict]]:
    tabelas = set(args.tabelas.split(",")) if args.tabelas else set(ARQUIVOS)
    if args.ordem == "tabela":
        ordem = sorted(ordem, key=lambda e: (list(ARQUIVOS).index(e[0]) if e[0] in ARQUIVOS else 99, e[1]))
    elif args.ordem == "aleatoria":
        ordem = list(ordem)
        random.Random(args.seed).shuffle(ordem)
    ini = args.pular_escolas
    fim = ini + args.escolas if args.escolas else None
    eventos = []
    for tab, i, seq, tipo in ordem:
        if seq < ini or (fim is not None and seq >= fim):
            continue
        if tab == "malformada":
            if args.sem_malformadas:
                continue
            m = malformadas[i]
            if m["tabela"] not in tabelas:
                continue
            eventos.append((m["tabela"], m["payload_bruto"].encode("utf-8"),
                            {"ref": f"malformada:{i}", "escola_seq": str(seq)}))
        elif tab in tabelas:
            eventos.append((tab, linhas[tab][i].encode("utf-8"),
                            {"ref": f"{tab}:{i}", "escola_seq": str(seq)}))
    return eventos


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--projeto")
    ap.add_argument("--topico", default="ideb-bronze-eventos")
    ap.add_argument("--dados", default=str(Path(__file__).resolve().parents[1] / "dados"),
                    help="pasta local ou gs://bucket/prefixo com bronze/ e fluxo/")
    ap.add_argument("--ordem", choices=["escola", "aleatoria", "tabela"], default="escola")
    ap.add_argument("--taxa", type=float, default=100.0, help="mensagens por segundo (0 = sem limite)")
    ap.add_argument("--escolas", type=int, default=0, help="quantas escolas enviar (0 = todas)")
    ap.add_argument("--pular-escolas", type=int, default=0, help="começa a partir da escola N")
    ap.add_argument("--tabelas", default="", help="subconjunto, ex.: ideb,inse")
    ap.add_argument("--sem-malformadas", action="store_true")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--dry-run", action="store_true", help="não publica; mostra contagens e exemplos")
    args = ap.parse_args()

    dados = baixar_do_gcs(args.dados) if args.dados.startswith("gs://") else Path(args.dados)
    linhas, malformadas, ordem = carregar(dados)
    eventos = montar_eventos(linhas, malformadas, ordem, args)
    lote = f"lote-{time.strftime('%Y%m%d-%H%M%S')}"
    por_tabela: dict[str, int] = {}
    for t, _, _ in eventos:
        por_tabela[t] = por_tabela.get(t, 0) + 1
    print(f"{len(eventos)} mensagens a enviar | {por_tabela} | ordem={args.ordem} | lote={lote}", flush=True)

    if args.dry_run:
        for t, dado, at in eventos[:5]:
            print(t, at, dado[:200].decode("utf-8", "replace"))
        return
    if not args.projeto:
        ap.error("informe --projeto")

    from google.cloud import pubsub_v1
    from google.cloud.pubsub_v1.types import BatchSettings, LimitExceededBehavior, PublishFlowControl, PublisherOptions

    pub = pubsub_v1.PublisherClient(
        batch_settings=BatchSettings(max_messages=500, max_bytes=5_000_000, max_latency=0.05),
        publisher_options=PublisherOptions(flow_control=PublishFlowControl(
            message_limit=5000, byte_limit=50_000_000, limit_exceeded_behavior=LimitExceededBehavior.BLOCK)),
    )
    topico = pub.topic_path(args.projeto, args.topico)
    falhas, futuros = [], []
    t0 = time.time()
    intervalo = 1.0 / args.taxa if args.taxa > 0 else 0.0
    proximo = time.time()
    for k, (tab, dado, at) in enumerate(eventos, 1):
        if intervalo:
            agora = time.time()
            if agora < proximo:
                time.sleep(proximo - agora)
            proximo = max(proximo + intervalo, time.time() - 1.0)
        fut = pub.publish(topico, dado, tabela=tab, evento_id=str(uuid.uuid4()), lote=lote, **at)
        futuros.append(fut)
        if k % 1000 == 0:
            dt = time.time() - t0
            print(f"  {k}/{len(eventos)} publicadas ({k / dt:.0f} msg/s)", flush=True)
    for f in futuros:
        try:
            f.result(timeout=120)
        except Exception as exc:  # noqa: BLE001
            falhas.append(exc)
    dt = time.time() - t0
    print(f"Concluído: {len(eventos) - len(falhas)} publicadas, {len(falhas)} falhas, {dt:.1f} s", flush=True)
    if falhas:
        print(f"Primeira falha: {falhas[0]}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
