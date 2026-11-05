-- Silver: dado limpo, tipado, deduplicado.
--
-- Histórico da decisão (relevante pra apresentação — mostra iteração real,
-- não decisão de primeira tentativa):
-- 1ª versão testada: manteve os percentuais por nível (percentual_nivel_1
--    a 8), motivada por teste de correlação com a média `inse`
--    (correlação moderada, ~-0.62 a +0.50 — não ±1.0 — indicando que os
--    percentuais carregam informação de heterogeneidade que a média
--    sozinha não captura).
-- Verificação feita em seguida: contagem de não-nulos por nível mostrou
--    que anos mais antigos (ex: 2013) têm os percentuais nulos mesmo com
--    `inse` preenchido.
-- Decisão final (esta versão): descartar os percentuais e manter só
--    `inse`, pra não perder cobertura temporal por causa de colunas com
--    alta taxa de nulo em anos antigos. Trade-off: simplicidade e
--    cobertura de anos vs. granularidade de distribuição socioeconômica.
--
-- Cobertura de anos: INSE só existe em 2013, 2015, 2019, 2021, 2023
-- (falta 2017, que existe no IDEB) — tratado na integração (Gold), não aqui.
--
-- Dedup por id_escola + ano, desempate por maior `quantidade_alunos_inse`
-- (mais alunos considerados no cálculo = medida mais confiável).

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.silver_inse_escola` AS
SELECT
  id_escola,
  ano,
  inse
FROM `platinum-avenue-505512-t5.trabalho_pdm.bronze_inse_escola`
WHERE inse IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola, ano ORDER BY quantidade_alunos_inse DESC) = 1;
