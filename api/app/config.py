"""Configuração da API, lida de variáveis de ambiente (definidas no gcloud run deploy)."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    backend: str            # savedmodel | vertex
    modelo_uri: str         # gs://BUCKET/modelo_ideb/v1 (contém modelo/ e metadados_modelo.json)
    projeto: str
    regiao: str
    dataset: str
    vertex_endpoint_id: str
    vertex_url: str         # só para testes locais: URL completa de um servidor que imita o Vertex AI
    dir_local: str

    @classmethod
    def do_ambiente(cls) -> "Config":
        return cls(
            backend=os.environ.get("BACKEND", "savedmodel").strip().lower(),
            modelo_uri=os.environ.get("MODELO_URI", "").strip(),
            projeto=os.environ.get("PROJETO", "platinum-avenue-505512-t5"),
            regiao=os.environ.get("REGIAO", "us-central1"),
            dataset=os.environ.get("DATASET", "trabalho_pdm"),
            vertex_endpoint_id=os.environ.get("VERTEX_ENDPOINT_ID", "").strip(),
            vertex_url=os.environ.get("VERTEX_URL", "").strip(),
            dir_local=os.environ.get("DIR_LOCAL_MODELO", "/tmp/modelo_ideb"),
        )
