-- Bronze: cópia fiel da fonte, sem alteração.
-- Fonte: basedosdados.br_inep_censo_escolar.docente (dataset público)
-- Grão de origem: professor + turma + etapa de ensino (não escola).
-- Tabela grande (~134M linhas) — via CREATE TABLE AS SELECT * em vez de COPY.

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.bronze_censo_docente` AS
SELECT * FROM `basedosdados.br_inep_censo_escolar.docente`;
