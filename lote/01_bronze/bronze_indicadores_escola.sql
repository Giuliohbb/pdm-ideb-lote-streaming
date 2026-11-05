-- Bronze: cópia fiel da fonte, sem alteração.
-- Fonte: basedosdados.br_inep_indicadores_educacionais.escola (dataset público)
-- Indicadores complementares do INEP: ATU, HAD, TDI, IED, AFD, IRD, ICG.
-- Entram na Gold porque são indicadores já calculados e validados pelo
-- próprio INEP, complementares às características brutas do censo.

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.bronze_indicadores_escola`
COPY `basedosdados.br_inep_indicadores_educacionais.escola`;
