"""Serviço de limpeza em tempo real (Cloud Run) alimentado por uma assinatura push do Pub/Sub.

Fluxo de cada requisição:
  1. o Pub/Sub entrega UMA mensagem (envelope JSON com o payload em base64 e os atributos);
  2. o payload é decodificado; JSON inválido vai para a quarentena (G06);
  3. o atributo `tabela` escolhe a função de limpeza (regras_limpeza.py);
  4. registro aceito -> linha na silver de streaming da tabela, com metadados de linhagem;
  5. toda mensagem, qualquer que seja o destino, gera uma linha na auditoria;
  6. resposta 204 confirma a mensagem (ack). Falha ao gravar no BigQuery -> 500, e o Pub/Sub
     reentrega com backoff; depois de 5 tentativas a mensagem vai para o tópico de dead letter.

Idempotência: o messageId do Pub/Sub é usado como insertId no BigQuery, o que descarta
(em regime de melhor esforço) as regravações da mesma mensagem. Duplicatas de negócio
(o mesmo registro enviado duas vezes) são resolvidas depois, nas views de consolidação.

Variáveis de ambiente: PROJETO, DATASET (padrão trabalho_pdm).
Execução local: PROJETO=x DATASET=y python servico_limpeza.py
"""
from __future__ import annotations

import base64
import binascii
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from flask import Flask, request

import esquema as E
import regras_limpeza as R

COLUNAS_BRONZE = {k: set(v) for k, v in
                  json.loads((Path(__file__).with_name("colunas_bronze.json")).read_text()).items()}
LIMITE_PAYLOAD_AUDITORIA = 20000


class Gravador(Protocol):
    def inserir(self, tabela: str, linhas: list[dict], ids: list[str]) -> None: ...


class GravadorBigQuery:
    """Gravação por streaming (insertAll) com insertId = messageId."""

    def __init__(self, projeto: str, dataset: str):
        from google.cloud import bigquery
        self._cli = bigquery.Client(project=projeto)
        self._prefixo = f"{projeto}.{dataset}."

    def inserir(self, tabela: str, linhas: list[dict], ids: list[str]) -> None:
        erros = self._cli.insert_rows_json(self._prefixo + tabela, linhas, row_ids=ids)
        if erros:
            raise RuntimeError(f"insert_rows_json em {tabela} falhou: {erros}")


def _agora() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(severidade: str, mensagem: str, **campos) -> None:
    """Log estruturado: o Cloud Logging lê o JSON e indexa os campos."""
    print(json.dumps({"severity": severidade, "message": mensagem, **campos}, ensure_ascii=False,
                     default=str), file=sys.stdout, flush=True)


def processar_mensagem(msg: dict, gravador: Gravador) -> R.Resultado:
    """Limpa uma mensagem do Pub/Sub e grava silver + auditoria. Exceções sobem para virar 500."""
    attrs = msg.get("attributes") or {}
    message_id = msg.get("messageId") or msg.get("message_id") or attrs.get("evento_id") or "sem-id"
    publish_time = msg.get("publishTime") or msg.get("publish_time") or _agora()
    tabela = attrs.get("tabela")

    texto = None
    try:
        texto = base64.b64decode(msg.get("data", ""), validate=False).decode("utf-8")
        registro = json.loads(texto)
        res = R.limpar(tabela, registro, COLUNAS_BRONZE)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        res = R.Resultado(R.QUARENTENA, None, ["G06"], f"JSON malformado: {exc.__class__.__name__}")

    agora = _agora()
    if res.status == R.ACEITO:
        linha = dict(res.registro, _message_id=message_id, _publish_time=publish_time,
                     _ingestao_ts=agora, _regras_aplicadas=res.regras)
        gravador.inserir(E.TABELA_SILVER_STREAM[tabela], [linha], [message_id])

    auditoria = {
        "message_id": message_id, "publish_time": publish_time, "processado_em": agora,
        "tabela": tabela, "ref": attrs.get("ref"), "id_escola": res.id_escola, "status": res.status,
        "motivo": res.motivo, "regras_aplicadas": res.regras,
        "payload": (texto or "")[:LIMITE_PAYLOAD_AUDITORIA] if res.status == R.QUARENTENA else None,
    }
    gravador.inserir(E.TABELA_AUDITORIA, [auditoria], [message_id])
    return res


def criar_app(gravador: Gravador | None = None) -> Flask:
    app = Flask(__name__)
    estado = {"gravador": gravador}

    def _grav() -> Gravador:
        if estado["gravador"] is None:
            estado["gravador"] = GravadorBigQuery(os.environ["PROJETO"], os.environ.get("DATASET", "trabalho_pdm"))
        return estado["gravador"]

    @app.get("/")
    def saude():
        return {"servico": "limpeza-ideb", "status": "ok"}

    @app.post("/")
    @app.post("/pubsub")
    def receber():
        envelope = request.get_json(silent=True)
        if not envelope or "message" not in envelope:
            _log("WARNING", "requisicao sem envelope do Pub/Sub")
            return ("envelope invalido", 400)
        msg = envelope["message"]
        try:
            res = processar_mensagem(msg, _grav())
        except Exception as exc:  # BigQuery indisponível, cota etc.: o Pub/Sub reentrega
            _log("ERROR", "falha ao gravar; mensagem sera reentregue", erro=str(exc),
                 message_id=msg.get("messageId"))
            return ("erro temporario", 500)
        _log("INFO" if res.status == R.ACEITO else "NOTICE", f"{res.status}",
             tabela=(msg.get("attributes") or {}).get("tabela"), id_escola=res.id_escola,
             regras=res.regras, motivo=res.motivo, ref=(msg.get("attributes") or {}).get("ref"))
        return ("", 204)

    return app


app = criar_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
