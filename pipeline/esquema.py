"""Esquemas, domínios e faixas válidas compartilhados pelo pipeline de streaming.

Este módulo é a fonte única de verdade para:
  * as colunas de cada silver (as mesmas da fase 1, para manter a relação lote x streaming);
  * as colunas da gold, na ordem exata da `gold_ideb_features` usada no treino do modelo;
  * os domínios categóricos vistos no treino (o one-hot do BigQuery ML depende do texto exato);
  * as faixas plausíveis usadas pelas regras de validação.

Ele é importado pelo serviço de limpeza, pelo construtor da gold e pelos testes.
"""

# ---------------------------------------------------------------------------
# Tabelas de origem (atributo `tabela` das mensagens do Pub/Sub)
# ---------------------------------------------------------------------------
TABELAS = ("ideb", "censo_escola", "indicadores", "inse", "docente")

# Nome das tabelas silver de streaming no BigQuery, por tabela de origem
TABELA_SILVER_STREAM = {
    "ideb": "silver_stream_ideb_escola",
    "censo_escola": "silver_stream_censo_escola",
    "indicadores": "silver_stream_indicadores_escola",
    "inse": "silver_stream_inse_escola",
    "docente": "silver_stream_censo_docente",
}
# Nome dos arquivos/tabelas bronze (os mesmos nomes das bronzes da fase 1)
ARQUIVO_BRONZE = {
    "ideb": "bronze_ideb_escola",
    "censo_escola": "bronze_censo_escola",
    "indicadores": "bronze_indicadores_escola",
    "inse": "bronze_inse_escola",
    "docente": "bronze_censo_docente",
}
TABELA_BRONZE_STREAM = "bronze_stream_eventos"
TABELA_AUDITORIA = "auditoria_limpeza_stream"
TABELA_GOLD_STREAM = "gold_ideb_features_stream"
TABELA_GOLD_LOTE = "gold_ideb_features"
TABELA_PREDICOES = "predicoes_ideb_stream"

# Colunas de metadados acrescentadas pelo serviço de limpeza a cada linha silver
METADADOS_SILVER = [
    ("_message_id", "STRING"),
    ("_publish_time", "TIMESTAMP"),
    ("_ingestao_ts", "TIMESTAMP"),
    ("_regras_aplicadas", "ARRAY<STRING>"),
]

# ---------------------------------------------------------------------------
# UFs e códigos IBGE (2 primeiros dígitos de id_municipio e de id_escola)
# ---------------------------------------------------------------------------
CODIGO_UF = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}
UFS = set(CODIGO_UF.values())
REGIAO_UF = {
    **{uf: "Norte" for uf in ("AC", "AM", "AP", "PA", "RO", "RR", "TO")},
    **{uf: "Nordeste" for uf in ("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE")},
    **{uf: "Sudeste" for uf in ("ES", "MG", "RJ", "SP")},
    **{uf: "Sul" for uf in ("PR", "RS", "SC")},
    **{uf: "Centro-Oeste" for uf in ("DF", "GO", "MS", "MT")},
}

# ---------------------------------------------------------------------------
# Silver IDEB (fase 1: id_escola, ano, ideb)
# ---------------------------------------------------------------------------
SILVER_IDEB = [("id_escola", "STRING"), ("ano", "INT64"), ("ideb", "FLOAT64")]
IDEB_ENSINO_ALVO = "fundamental"
IDEB_ANOS_ESCOLARES_ALVO = "finais (6-9)"
# Componentes matemáticos do IDEB (vazamento): descartados pela projeção
IDEB_COLUNAS_VAZAMENTO = [
    "taxa_aprovacao", "indicador_rendimento", "nota_saeb_matematica",
    "nota_saeb_lingua_portuguesa", "nota_saeb_media_padronizada", "projecao",
]

# ---------------------------------------------------------------------------
# Silver Censo Escola (fase 1: 64 colunas, mesma ordem e nomes)
# (coluna_bronze, coluna_silver, tipo)
# ---------------------------------------------------------------------------
CENSO_CONTEXTO = [
    ("sigla_uf", "sigla_uf", "STRING"),
    ("id_municipio", "id_municipio", "STRING"),
    ("rede", "rede", "STRING"),
    ("tipo_localizacao", "tipo_localizacao", "STRING"),
    ("tipo_localizacao_diferenciada", "tipo_localizacao_diferenciada", "STRING"),
]
_CENSO_ATRIBUTOS_NOMES = [
    # infraestrutura predial
    "local_funcionamento_predio_escolar", "predio_compartilhado", "agua_potavel",
    "energia_rede_publica", "esgoto_rede_publica", "lixo_servico_coleta",
    # espaços pedagógicos
    "biblioteca", "laboratorio_ciencias", "laboratorio_informatica", "quadra_esportes",
    "patio_coberto", "refeitorio", "cozinha", "sala_multiuso", "banheiro", "banheiro_pne",
    "area_verde", "auditorio", "sala_atendimento_especial", "dependencia_pne",
    "acessibilidade_inexistente",
    # capacidade física
    "quantidade_sala_existente", "quantidade_sala_utilizada_climatizada",
    # tecnologia
    "equipamento_computador", "equipamento_lousa_digital", "equipamento_multimidia",
    "quantidade_computador_aluno", "quantidade_computador", "internet", "internet_alunos",
    "banda_larga",
    # corpo técnico e apoio
    "quantidade_funcionario", "profissional_coordenador", "profissional_psicologo",
    "profissional_assistente_social", "profissional_bibliotecario", "alimentacao",
    # seleção e turno
    "exame_selecao", "diurno", "noturno",
    # outras etapas atendidas
    "etapa_ensino_fundamental_anos_iniciais", "etapa_ensino_medio", "etapa_ensino_eja",
    "etapa_ensino_especial",
]
_CENSO_RENOMEADAS = [
    ("quantidade_matricula_fundamental_anos_finais", "qtd_matricula_anos_finais"),
    ("quantidade_matricula_fundamental", "qtd_matricula_fundamental"),
    ("quantidade_matricula_feminino", "qtd_matricula_feminino"),
    ("quantidade_matricula_masculino", "qtd_matricula_masculino"),
    ("quantidade_matricula_branca", "qtd_matricula_branca"),
    ("quantidade_matricula_preta", "qtd_matricula_preta"),
    ("quantidade_matricula_parda", "qtd_matricula_parda"),
    ("quantidade_matricula_indigena", "qtd_matricula_indigena"),
    ("quantidade_matricula_zona_residencia_rural", "qtd_matricula_zona_rural"),
    ("quantidade_matricula_utiliza_transporte_publico", "qtd_matricula_transporte_publico"),
    ("quantidade_docente_fundamental_anos_finais", "qtd_docente_anos_finais"),
    ("quantidade_docente_educacao_basica", "qtd_docente_educacao_basica"),
    ("quantidade_turma_fundamental_anos_finais", "qtd_turma_anos_finais"),
]
CENSO_ATRIBUTOS = [(c, c, "INT64") for c in _CENSO_ATRIBUTOS_NOMES] + [
    (b, s, "INT64") for b, s in _CENSO_RENOMEADAS
]
SILVER_CENSO = (
    [("id_escola", "STRING"), ("ano", "INT64")]
    + [(s, t) for _, s, t in CENSO_CONTEXTO]
    + [(s, t) for _, s, t in CENSO_ATRIBUTOS]
)


def censo_eh_quantidade(coluna_bronze: str) -> bool:
    """Atributos de contagem (os demais são flags 0/1)."""
    return coluna_bronze.startswith("quantidade_")


# ---------------------------------------------------------------------------
# Silver Indicadores (fase 1: 11 indicadores)
# ---------------------------------------------------------------------------
INDICADORES_NUMERICOS = [
    "atu_ef_anos_finais", "had_ef_anos_finais", "afd_ef_anos_finais_grupo_1",
    "ird_media_regularidade_docente",
    "ied_ef_anos_finais_nivel_1", "ied_ef_anos_finais_nivel_2", "ied_ef_anos_finais_nivel_3",
    "ied_ef_anos_finais_nivel_4", "ied_ef_anos_finais_nivel_5", "ied_ef_anos_finais_nivel_6",
]
INDICADOR_ICG = "icg_nivel_complexidade_gestao_escola"
SILVER_INDICADORES = (
    [("id_escola", "STRING"), ("ano", "INT64")]
    + [(c, "FLOAT64") for c in INDICADORES_NUMERICOS]
    + [(INDICADOR_ICG, "STRING")]
)
# Faixas plausíveis: valor fora da faixa vira nulo (o indicador não é COALESCEado na fase 1)
FAIXA_INDICADOR = {
    "atu_ef_anos_finais": (1.0, 100.0),
    "had_ef_anos_finais": (1.0, 24.0),
    "afd_ef_anos_finais_grupo_1": (0.0, 100.0),
    "ird_media_regularidade_docente": (1.0, 5.0),
    **{f"ied_ef_anos_finais_nivel_{i}": (0.0, 100.0) for i in range(1, 7)},
}

# ---------------------------------------------------------------------------
# Silver INSE (fase 1: id_escola, ano, inse; dedup pela maior quantidade_alunos_inse)
# No streaming a quantidade fica na silver porque a deduplicação acontece na consolidação.
# ---------------------------------------------------------------------------
SILVER_INSE = [
    ("id_escola", "STRING"), ("ano", "INT64"), ("inse", "FLOAT64"),
    ("quantidade_alunos_inse", "INT64"),
]
FAIXA_INSE = (1.0, 9.0)  # escala compatibilizada pela Base dos Dados (observado: 2,4 a 6,6)

# ---------------------------------------------------------------------------
# Silver Docente (grão: docente x turma). A agregação por escola acontece na consolidação.
# ---------------------------------------------------------------------------
SILVER_DOCENTE = [
    ("id_escola", "STRING"), ("ano", "INT64"), ("id_docente", "STRING"),
    ("id_turma", "STRING"), ("etapa_ensino", "STRING"), ("idade", "INT64"),
    ("escolaridade", "STRING"), ("licenciatura_1", "INT64"), ("especializacao", "INT64"),
    ("mestrado", "INT64"), ("doutorado", "INT64"), ("tipo_contratacao", "STRING"),
    ("tipo_docente", "INT64"), ("disciplina_matematica", "INT64"),
    ("disciplina_lingua_portuguesa", "INT64"),
]
DOCENTE_FLAGS = [
    "licenciatura_1", "especializacao", "mestrado", "doutorado",
    "disciplina_matematica", "disciplina_lingua_portuguesa",
]
# Etapas do Fundamental Anos Finais no Censo 2019+: 6º, 7º, 8º e 9º ano (EF de 9 anos)
ETAPAS_ANOS_FINAIS = {"19", "20", "21", "41"}
FAIXA_IDADE_DOCENTE = (16, 90)

# Agregado de docentes por escola/ano (mesmos nomes e tipos da silver_censo_docente_agregado
# em produção). (coluna, expressão BigQuery sobre as linhas já deduplicadas)
DOCENTE_AGREGADO = [
    ("qtd_docente_anos_finais", "INT64", "COUNT(DISTINCT id_docente)"),
    ("idade_media_docente", "FLOAT64", "ROUND(AVG(idade), 1)"),
    ("qtd_docente_escolaridade_superior", "INT64", "COUNT(DISTINCT IF(escolaridade = '4', id_docente, NULL))"),
    ("qtd_docente_licenciatura", "INT64", "COUNT(DISTINCT IF(licenciatura_1 = 1, id_docente, NULL))"),
    ("qtd_docente_especializacao", "INT64", "COUNT(DISTINCT IF(especializacao = 1, id_docente, NULL))"),
    ("qtd_docente_mestrado", "INT64", "COUNT(DISTINCT IF(mestrado = 1, id_docente, NULL))"),
    ("qtd_docente_doutorado", "INT64", "COUNT(DISTINCT IF(doutorado = 1, id_docente, NULL))"),
    ("qtd_docente_efetivo", "INT64", "COUNT(DISTINCT IF(tipo_contratacao = '1', id_docente, NULL))"),
    ("qtd_docente_tipo_docente_regular", "INT64", "COUNT(DISTINCT IF(tipo_docente = 1, id_docente, NULL))"),
    ("qtd_docente_disciplina_matematica", "INT64", "COUNT(DISTINCT IF(disciplina_matematica = 1, id_docente, NULL))"),
    ("qtd_docente_disciplina_lingua_portuguesa", "INT64", "COUNT(DISTINCT IF(disciplina_lingua_portuguesa = 1, id_docente, NULL))"),
]

# ---------------------------------------------------------------------------
# Gold: as 87 colunas da gold_ideb_features, na ordem do DDL de produção.
# O modelo foi treinado com SELECT * EXCEPT(id_escola), então a gold de streaming
# precisa ter exatamente estes nomes e tipos.
# ---------------------------------------------------------------------------
GOLD = (
    [("id_escola", "STRING"), ("ano", "INT64"), ("ideb", "FLOAT64")]
    + [(s, t) for _, s, t in CENSO_CONTEXTO]
    + [(s, t) for _, s, t in CENSO_ATRIBUTOS if s != "qtd_docente_anos_finais"]
    + [(c, "FLOAT64") for c in INDICADORES_NUMERICOS]
    + [(INDICADOR_ICG, "STRING")]
    + [("inse", "FLOAT64")]
    + [(c, t) for c, t, _ in DOCENTE_AGREGADO]
)
GOLD_COLUNAS = [c for c, _ in GOLD]
assert len(GOLD) == 87, len(GOLD)

# ---------------------------------------------------------------------------
# Domínios categóricos (exatamente como aparecem no treino do modelo_ideb)
# ---------------------------------------------------------------------------
REDE_ALIAS = {
    "1": "1", "federal": "1",
    "2": "2", "estadual": "2",
    "3": "3", "municipal": "3",
    "4": "4", "privada": "4", "particular": "4",
}
LOCALIZACAO_ALIAS = {"1": "1", "urbana": "1", "urbano": "1", "2": "2", "rural": "2"}
# tipo_localizacao_diferenciada chega da Base dos Dados como "0.0", "1.0", ... (texto de float)
LOCALIZACAO_DIFERENCIADA_VALIDOS = {f"{i}.0" for i in range(0, 8)}
ICG_VALIDOS = {f"nivel {i}" for i in range(1, 7)}

ESCOLARIDADE_ALIAS = {
    "fundamental incompleto": "1",
    "fundamental completo": "2",
    "ensino medio": "3", "medio": "3", "ensino medio completo": "3",
    "ensino superior completo": "4", "superior completo": "4", "superior": "4",
    "ensino superior": "4",
}
CONTRATACAO_ALIAS = {
    "concursado/efetivo/estavel": "1", "concursado": "1", "efetivo": "1", "estavel": "1",
    "contrato temporario": "2", "temporario": "2",
    "contrato terceirizado": "3", "terceirizado": "3",
    "contrato clt": "4", "clt": "4",
}

FAIXA_IDEB = (0.0, 10.0)
FAIXA_ANO = (2005, 2030)
