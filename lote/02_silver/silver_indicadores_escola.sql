-- Silver: dado limpo, tipado, deduplicado.
--
-- Fonte complementar do INEP com indicadores já calculados a nível de
-- escola: ATU (adequação da formação docente), HAD (média de horas-aula
-- diárias), AFD (esforço docente), IRD (regularidade docente), IED
-- (esforço docente por nível de complexidade) e ICG (complexidade de
-- gestão da escola).
--
-- Por que entra: indicadores prontos e validados pelo próprio INEP,
-- complementares às características brutas do censo.
--
-- Dedup por id_escola + ano via QUALIFY ROW_NUMBER().

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.silver_indicadores_escola` AS
SELECT
  id_escola,
  ano,
  atu_ef_anos_finais,
  had_ef_anos_finais,
  afd_ef_anos_finais_grupo_1,
  ird_media_regularidade_docente,
  ied_ef_anos_finais_nivel_1,
  ied_ef_anos_finais_nivel_2,
  ied_ef_anos_finais_nivel_3,
  ied_ef_anos_finais_nivel_4,
  ied_ef_anos_finais_nivel_5,
  ied_ef_anos_finais_nivel_6,
  icg_nivel_complexidade_gestao_escola
FROM `basedosdados.br_inep_indicadores_educacionais.escola`
WHERE id_escola IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola, ano ORDER BY id_escola) = 1;
