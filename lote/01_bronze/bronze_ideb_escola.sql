-- Bronze: cópia fiel da fonte, sem alteração.
-- Fonte: basedosdados.br_inep_ideb.escola (dataset público)

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.bronze_ideb_escola`
COPY `basedosdados.br_inep_ideb.escola`;
