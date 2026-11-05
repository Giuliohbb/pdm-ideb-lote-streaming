"""API REST de predição do IDEB (Trabalho 2 de PDM, UFG).

Rotas
  POST /predicao                    recebe de 1 a 1000 escolas (85 features cada) e devolve o IDEB previsto
  GET  /escolas/{id_escola}/predicao busca a escola na gold (lote e streaming) e prevê na hora
  GET  /modelo                      versão, origem, métricas oficiais e assinatura do modelo carregado
  GET  /saude                       200 quando o modelo está carregado, 503 caso contrário
  GET  /docs                        documentação interativa (Swagger), gerada automaticamente

Execução local:  MODELO_URI=caminho/ou/gs://... uvicorn app.main:app --port 8080
"""
from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import fonte_bigquery
from .config import Config
from .esquema_api import (ErroEntrada, InfoModelo, PedidoPredicao, Predicao, RespostaPredicao,
                          preparar)
from .preditor import ErroBackend, criar_preditor


# ---------------------------------------------------------------------------
# Logs estruturados: uma linha JSON por evento, que o Cloud Logging indexa campo a campo
# ---------------------------------------------------------------------------
class _FormatoJson(logging.Formatter):
    def format(self, rec: logging.LogRecord) -> str:
        d = {"severity": rec.levelname, "message": rec.getMessage(), "componente": "api-ideb"}
        d.update(getattr(rec, "campos", {}) or {})
        if rec.exc_info:
            d["excecao"] = self.formatException(rec.exc_info)
        return json.dumps(d, ensure_ascii=False, default=str)


_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(_FormatoJson())
log = logging.getLogger("api_ideb")
log.handlers[:] = [_h]
log.setLevel(logging.INFO)
log.propagate = False

ESTADO: dict = {"cfg": None, "preditor": None, "meta": None, "erro": None, "carregado_em": None}


def carregar() -> None:
    cfg = Config.do_ambiente()
    ESTADO.update(cfg=cfg, preditor=None, meta=None, erro=None)
    try:
        ESTADO["preditor"], ESTADO["meta"] = criar_preditor(cfg)
        ESTADO["carregado_em"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        log.info("modelo pronto", extra={"campos": {"backend": cfg.backend, "modelo_uri": cfg.modelo_uri}})
    except Exception as e:  # noqa: BLE001  (a API sobe mesmo assim e responde 503 em /saude)
        ESTADO["erro"] = f"{type(e).__name__}: {e}"
        log.exception("falha ao carregar o modelo", extra={"campos": {"backend": cfg.backend}})


@asynccontextmanager
async def ciclo_de_vida(_app: FastAPI):
    carregar()
    yield


app = FastAPI(
    title="API de predição do IDEB",
    version="1.0.0",
    description=("Serve o modelo_ideb (regressão linear do BigQuery ML, fase 1) exportado como "
                 "TensorFlow SavedModel e registrado no Vertex AI Model Registry. Recebe as 85 "
                 "features de uma escola (as colunas da gold_ideb_features menos id_escola e ideb) "
                 "e devolve o IDEB previsto dos anos finais do ensino fundamental."),
    lifespan=ciclo_de_vida,
)


def _preditor():
    if ESTADO["preditor"] is None:
        raise HTTPException(status_code=503, detail=f"modelo indisponível: {ESTADO['erro']}")
    return ESTADO["preditor"]


def _info_modelo() -> InfoModelo:
    meta, p = ESTADO["meta"], ESTADO["preditor"]
    return InfoModelo(nome=meta.nome if meta else "modelo_ideb",
                      versao=meta.versao if meta else "desconhecida",
                      backend=p.backend if p else "nenhum")


TRADUCAO_ERROS = {
    "missing": "campo obrigatório ausente (pode ser null, mas precisa vir)",
    "int_parsing": "deve ser um número inteiro",
    "int_from_float": "deve ser um número inteiro, sem casas decimais",
    "int_type": "deve ser um número inteiro",
    "float_parsing": "deve ser um número",
    "float_type": "deve ser um número",
    "string_type": "deve ser texto",
    "extra_forbidden": "campo desconhecido: não é uma das 85 features do modelo",
    "too_short": "a lista precisa ter pelo menos 1 escola",
    "too_long": "a lista aceita no máximo 1.000 escolas por requisição",
    "list_type": "deve ser uma lista",
    "model_attributes_type": "cada escola deve ser um objeto JSON",
    "json_invalid": "o corpo não é um JSON válido",
    "string_pattern_mismatch": "formato inválido",
    "greater_than_equal": "valor abaixo do mínimo permitido",
    "less_than_equal": "valor acima do máximo permitido",
}


@app.exception_handler(RequestValidationError)
async def _entrada_invalida(_req: Request, exc: RequestValidationError):
    detalhes = []
    for e in exc.errors():
        d = {"campo": ".".join(str(x) for x in e.get("loc", []) if x != "body"),
             "problema": TRADUCAO_ERROS.get(e.get("type"), e.get("msg"))}
        if e.get("type") not in ("missing", "too_long", "too_short"):
            d["recebido"] = e.get("input")
        detalhes.append(d)
    log.info("entrada inválida", extra={"campos": {"erros": len(detalhes)}})
    return JSONResponse(status_code=422, content=json.loads(json.dumps(
        {"erro": "entrada inválida", "quantidade_erros": len(detalhes), "detalhes": detalhes[:50]},
        default=str)))


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------
@app.get("/", summary="Apresentação da API")
def raiz():
    return {"servico": "API de predição do IDEB", "documentacao": "/docs",
            "rotas": ["POST /predicao", "GET /escolas/{id_escola}/predicao", "GET /modelo", "GET /saude"]}


@app.get("/saude", summary="Verificação de saúde")
def saude():
    if ESTADO["preditor"] is None:
        return JSONResponse(status_code=503, content={"status": "indisponivel", "erro": ESTADO["erro"]})
    return {"status": "ok", "modelo": _info_modelo().model_dump(), "carregado_em": ESTADO["carregado_em"]}


@app.get("/modelo", summary="Metadados do modelo em produção")
def modelo():
    p = _preditor()
    meta = ESTADO["meta"]
    cfg = ESTADO["cfg"]
    return {
        "modelo": _info_modelo().model_dump(),
        "origem_artefato": cfg.modelo_uri,
        "vertex_endpoint_id": cfg.vertex_endpoint_id or None,
        "carregado_em": ESTADO["carregado_em"],
        "assinatura": p.assinatura(),
        "metadados": meta.resumo() if meta else None,
    }


@app.post("/predicao", response_model=RespostaPredicao, summary="Prevê o IDEB de uma ou mais escolas")
def predicao(pedido: PedidoPredicao):
    inicio = time.perf_counter()
    p = _preditor()
    try:
        linhas, avisos = preparar(pedido.instancias, ESTADO["meta"])
        previstos = p.prever(linhas)
    except ErroEntrada as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except ErroBackend as e:
        log.error("falha no backend", extra={"campos": {"erro": str(e)}})
        raise HTTPException(status_code=502, detail=str(e)) from e
    saida = []
    for i, (inst, prev, av) in enumerate(zip(pedido.instancias, previstos, avisos)):
        real = getattr(inst, "ideb", None)
        saida.append(Predicao(indice=i, id_escola=getattr(inst, "id_escola", None), ideb_previsto=round(prev, 6),
                              ideb_informado=real,
                              erro_absoluto=round(abs(prev - real), 6) if real is not None else None,
                              avisos=av))
    ms = round((time.perf_counter() - inicio) * 1000, 2)
    log.info("predicao", extra={"campos": {"rota": "/predicao", "instancias": len(saida), "latencia_ms": ms,
                                            "backend": p.backend,
                                            "com_avisos": sum(1 for a in avisos if a)}})
    return RespostaPredicao(modelo=_info_modelo(), quantidade=len(saida), predicoes=saida, latencia_ms=ms)


@app.get("/escolas/{id_escola}/predicao", summary="Busca a escola na gold (lote e streaming) e prevê")
def predicao_escola(id_escola: str = Path(..., pattern=r"^\d{8}$", description="Código INEP, 8 dígitos"),
                    ano: Optional[int] = Query(None, ge=2005, le=2030)):
    inicio = time.perf_counter()
    p = _preditor()
    cfg = ESTADO["cfg"]
    try:
        linhas_gold = fonte_bigquery.buscar_escola(cfg.projeto, cfg.dataset, id_escola, ano)
    except Exception as e:  # noqa: BLE001
        log.exception("falha ao consultar o BigQuery")
        raise HTTPException(status_code=502, detail=f"falha ao consultar o BigQuery: {e}") from e
    if not linhas_gold:
        raise HTTPException(status_code=404, detail=f"escola {id_escola} não encontrada na gold"
                                                    + (f" para {ano}" if ano else ""))
    try:
        linhas, avisos = preparar(linhas_gold, ESTADO["meta"])
        previstos = p.prever(linhas)
    except ErroEntrada as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except ErroBackend as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    resultado = []
    for g, prev, av in zip(linhas_gold, previstos, avisos):
        bq = g.get("ideb_previsto_bigquery")
        resultado.append({
            "origem_dado": g["origem_dado"], "ano": g["ano"], "sigla_uf": g["sigla_uf"], "rede": g["rede"],
            "ideb_real": g["ideb"], "ideb_previsto_api": round(prev, 6),
            "ideb_previsto_bigquery": None if bq is None else round(float(bq), 6),
            "diferenca_api_bigquery": None if bq is None else round(abs(prev - float(bq)), 6),
            "avisos": av,
        })
    ms = round((time.perf_counter() - inicio) * 1000, 2)
    log.info("predicao_escola", extra={"campos": {"rota": "/escolas", "id_escola": id_escola,
                                                   "linhas": len(resultado), "latencia_ms": ms}})
    return {"id_escola": id_escola, "modelo": _info_modelo().model_dump(), "resultados": resultado,
            "latencia_ms": ms}
