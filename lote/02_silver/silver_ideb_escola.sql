-- Silver: dado limpo, tipado, deduplicado — fiel à fonte, sem decisões de
-- integração (essas ficam pra Gold).
--
-- Decisões:
-- - Filtro de etapa: ensino = "fundamental", anos_escolares = "finais (6-9)".
--   A tabela bronze tem granularidade escola+ano+ETAPA; sem esse filtro,
--   uma mesma escola apareceria várias vezes por ano (uma linha por etapa
--   oferecida), duplicando no join da Gold.
-- - ideb IS NOT NULL: ~38% das linhas têm IDEB nulo — o INEP suprime o
--   cálculo em turmas com poucos participantes na prova (cohort mínimo).
--   Isso tende a concentrar a supressão em escolas pequenas/rurais —
--   possível viés de seleção, documentar como limitação.
-- - Colunas descartadas (taxa_aprovacao, indicador_rendimento, notas SAEB,
--   projecao): são componentes matemáticos do próprio IDEB
--   (IDEB ≈ indicador_rendimento × nota_saeb_media_padronizada). Usá-las
--   como feature seria o modelo "decorar a fórmula" em vez de aprender
--   relação causal com características da escola (vazamento de dado).
-- - Dedup por id_escola + ano via QUALIFY ROW_NUMBER().

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.silver_ideb_escola` AS

SELECT
  id_escola,
  ano,
  ideb
FROM `platinum-avenue-505512-t5.trabalho_pdm.bronze_ideb_escola`

WHERE ensino = "fundamental"
  AND anos_escolares = "finais (6-9)"
  AND ideb IS NOT NULL

QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola, ano ORDER BY ideb DESC) = 1;
