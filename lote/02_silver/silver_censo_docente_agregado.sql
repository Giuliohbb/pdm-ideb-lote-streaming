-- Silver: agregação do corpo docente por escola/ano.
--
-- Grão de origem: docente x turma x etapa (não escola). Um mesmo docente
-- aparece em várias linhas quando leciona em várias turmas, por isso toda
-- contagem usa COUNT(DISTINCT id_docente): evita viesar o perfil a favor
-- de quem dá aula em mais turmas.
--
-- Recorte: só turmas do Fundamental Anos Finais (etapas 19, 20, 21 e 41 =
-- 6º, 7º, 8º e 9º ano do EF de 9 anos), coerente com o recorte do IDEB.
-- Códigos do censo: escolaridade '4' = ensino superior; tipo_contratacao
-- '1' = efetivo (concursado); tipo_docente 1 = docente titular
-- (exclui auxiliares, tradutores e tutores).
--
-- Procedência: o SQL original desta tabela não ficou no histórico de jobs
-- do grupo. Esta versão foi reconstruída no Trabalho 2: nomes, tipos e
-- ordem das colunas são os da tabela em produção (semente/ddl_tabelas.csv,
-- conferido em testes/teste_ponta_a_ponta.py) e as expressões foram
-- conferidas contra as estatísticas reais da tabela. É a mesma lógica da
-- view de streaming vw_silver_stream_censo_docente_agregado (sql/02_views.sql).

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.silver_censo_docente_agregado` AS
SELECT
  id_escola,
  ano,
  COUNT(DISTINCT id_docente) AS qtd_docente_anos_finais,
  ROUND(AVG(idade), 1) AS idade_media_docente,
  COUNT(DISTINCT IF(escolaridade = '4', id_docente, NULL)) AS qtd_docente_escolaridade_superior,
  COUNT(DISTINCT IF(licenciatura_1 = 1, id_docente, NULL)) AS qtd_docente_licenciatura,
  COUNT(DISTINCT IF(especializacao = 1, id_docente, NULL)) AS qtd_docente_especializacao,
  COUNT(DISTINCT IF(mestrado = 1, id_docente, NULL)) AS qtd_docente_mestrado,
  COUNT(DISTINCT IF(doutorado = 1, id_docente, NULL)) AS qtd_docente_doutorado,
  COUNT(DISTINCT IF(tipo_contratacao = '1', id_docente, NULL)) AS qtd_docente_efetivo,
  COUNT(DISTINCT IF(tipo_docente = 1, id_docente, NULL)) AS qtd_docente_tipo_docente_regular,
  COUNT(DISTINCT IF(disciplina_matematica = 1, id_docente, NULL)) AS qtd_docente_disciplina_matematica,
  COUNT(DISTINCT IF(disciplina_lingua_portuguesa = 1, id_docente, NULL)) AS qtd_docente_disciplina_lingua_portuguesa
FROM `platinum-avenue-505512-t5.trabalho_pdm.bronze_censo_docente`
WHERE id_escola IS NOT NULL
  AND etapa_ensino IN ('19', '20', '21', '41')
GROUP BY id_escola, ano;
