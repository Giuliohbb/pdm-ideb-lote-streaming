-- Gold: junção das 5 Silvers pela chave id_escola + ano, pronta pra
-- treinar o modelo.
--
-- Estratégia de join: JOIN simples (= INNER JOIN) em todas as 5 tabelas.
-- Isso é uma decisão real com uma consequência importante e mensurável:
-- silver_ideb_escola sozinha tem 311.340 linhas, mas gold_ideb_features
-- tem só 84.998 (~27% do total) — a interseção das 5 fontes. O maior
-- fator dessa queda é a cobertura de INSE, que só existe em
-- 2013/2015/2019/2021/2023 (contra ~10 biênios do IDEB); indicadores e
-- docente agregado também não cobrem 100% das combinações escola/ano.
--
-- Trade-off assumido (implícito na escolha de INNER JOIN em tudo):
-- dataset "completo" sem NULL em nenhuma feature, ao custo de cobertura
-- temporal reduzida à interseção das fontes. Vale mencionar isso na
-- apresentação como uma decisão consciente de troca entre volume de
-- dado e qualidade/completude de feature — não um bug.

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features` AS
SELECT
  ideb.id_escola,
  ideb.ano,
  ideb.ideb,

  -- características estruturais da escola
  censo.* EXCEPT(id_escola, ano, qtd_docente_anos_finais),

  -- indicadores complementares (ATU, HAD, AFD, IRD, IED, ICG)
  ind.* EXCEPT(id_escola, ano),

  -- nível socioeconômico
  inse.* EXCEPT(id_escola, ano),

  -- corpo docente agregado
  doc.* EXCEPT(id_escola, ano)

FROM `platinum-avenue-505512-t5.trabalho_pdm.silver_ideb_escola` AS ideb
JOIN `platinum-avenue-505512-t5.trabalho_pdm.silver_censo_escola` AS censo
  USING (id_escola, ano)
JOIN `platinum-avenue-505512-t5.trabalho_pdm.silver_indicadores_escola` AS ind
  USING (id_escola, ano)
JOIN `platinum-avenue-505512-t5.trabalho_pdm.silver_inse_escola` AS inse
  USING (id_escola, ano)
JOIN `platinum-avenue-505512-t5.trabalho_pdm.silver_censo_docente_agregado` AS doc
  USING (id_escola, ano);
