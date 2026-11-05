-- Bronze: cópia fiel da fonte, sem alteração.
-- Fonte: basedosdados.br_inep_censo_escolar.escola (dataset público, 455 colunas)

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.bronze_censo_escola`
COPY `basedosdados.br_inep_censo_escolar.escola`;
