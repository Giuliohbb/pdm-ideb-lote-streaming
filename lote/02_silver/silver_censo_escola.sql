-- Silver: dado limpo, tipado, deduplicado.
--
-- 455 colunas na origem, ~62 mantidas — selecionadas por bloco temático
-- (infraestrutura, espaços pedagógicos, tecnologia, corpo técnico, etc.),
-- não coluna por coluna. Blocos descartados por inteiro: vínculos
-- administrativos/CNPJ (burocrático, sem relação com desempenho
-- pedagógico), locais de funcionamento atípicos, dormitórios/piscina/
-- salas de nicho, organização curricular detalhada, reserva de vaga,
-- órgãos colegiados — baixo valor preditivo esperado ou granularidade
-- excessiva pro recorte (anos finais).
--
-- Tratamento de nulo — diferente do IDEB/INSE: aqui usamos COALESCE(0)
-- em vez de descartar a linha. No censo escolar (pesquisa declaratória),
-- ausência de valor tipicamente significa "a escola não possui aquele
-- item", não "dado desconhecido". Descartar a linha inteira perderia a
-- escola por causa de 1 entre ~60 campos.
--
-- Dedup por id_escola + ano via QUALIFY ROW_NUMBER().

CREATE OR REPLACE TABLE `platinum-avenue-505512-t5.trabalho_pdm.silver_censo_escola` AS
SELECT
  id_escola,
  ano,
  sigla_uf,
  id_municipio,
  rede,
  tipo_localizacao,
  tipo_localizacao_diferenciada,

  -- infraestrutura predial
  COALESCE(local_funcionamento_predio_escolar, 0) AS local_funcionamento_predio_escolar,
  COALESCE(predio_compartilhado, 0) AS predio_compartilhado,
  COALESCE(agua_potavel, 0) AS agua_potavel,
  COALESCE(energia_rede_publica, 0) AS energia_rede_publica,
  COALESCE(esgoto_rede_publica, 0) AS esgoto_rede_publica,
  COALESCE(lixo_servico_coleta, 0) AS lixo_servico_coleta,

  -- espaços pedagógicos
  COALESCE(biblioteca, 0) AS biblioteca,
  COALESCE(laboratorio_ciencias, 0) AS laboratorio_ciencias,
  COALESCE(laboratorio_informatica, 0) AS laboratorio_informatica,
  COALESCE(quadra_esportes, 0) AS quadra_esportes,
  COALESCE(patio_coberto, 0) AS patio_coberto,
  COALESCE(refeitorio, 0) AS refeitorio,
  COALESCE(cozinha, 0) AS cozinha,
  COALESCE(sala_multiuso, 0) AS sala_multiuso,
  COALESCE(banheiro, 0) AS banheiro,
  COALESCE(banheiro_pne, 0) AS banheiro_pne,
  COALESCE(area_verde, 0) AS area_verde,
  COALESCE(auditorio, 0) AS auditorio,
  COALESCE(sala_atendimento_especial, 0) AS sala_atendimento_especial,
  COALESCE(dependencia_pne, 0) AS dependencia_pne,
  COALESCE(acessibilidade_inexistente, 0) AS acessibilidade_inexistente,

  -- capacidade física
  COALESCE(quantidade_sala_existente, 0) AS quantidade_sala_existente,
  COALESCE(quantidade_sala_utilizada_climatizada, 0) AS quantidade_sala_utilizada_climatizada,

  -- tecnologia
  COALESCE(equipamento_computador, 0) AS equipamento_computador,
  COALESCE(equipamento_lousa_digital, 0) AS equipamento_lousa_digital,
  COALESCE(equipamento_multimidia, 0) AS equipamento_multimidia,
  COALESCE(quantidade_computador_aluno, 0) AS quantidade_computador_aluno,
  COALESCE(quantidade_computador, 0) AS quantidade_computador,
  COALESCE(internet, 0) AS internet,
  COALESCE(internet_alunos, 0) AS internet_alunos,
  COALESCE(banda_larga, 0) AS banda_larga,

  -- corpo técnico/apoio
  COALESCE(quantidade_funcionario, 0) AS quantidade_funcionario,
  COALESCE(profissional_coordenador, 0) AS profissional_coordenador,
  COALESCE(profissional_psicologo, 0) AS profissional_psicologo,
  COALESCE(profissional_assistente_social, 0) AS profissional_assistente_social,
  COALESCE(profissional_bibliotecario, 0) AS profissional_bibliotecario,
  COALESCE(alimentacao, 0) AS alimentacao,

  -- seleção e turno
  COALESCE(exame_selecao, 0) AS exame_selecao,
  COALESCE(diurno, 0) AS diurno,
  COALESCE(noturno, 0) AS noturno,

  -- outras etapas atendidas (contexto)
  COALESCE(etapa_ensino_fundamental_anos_iniciais, 0) AS etapa_ensino_fundamental_anos_iniciais,
  COALESCE(etapa_ensino_medio, 0) AS etapa_ensino_medio,
  COALESCE(etapa_ensino_eja, 0) AS etapa_ensino_eja,
  COALESCE(etapa_ensino_especial, 0) AS etapa_ensino_especial,

  -- matrícula (tamanho e composição, etapa anos finais)
  COALESCE(quantidade_matricula_fundamental_anos_finais, 0) AS qtd_matricula_anos_finais,
  COALESCE(quantidade_matricula_fundamental, 0) AS qtd_matricula_fundamental,
  COALESCE(quantidade_matricula_feminino, 0) AS qtd_matricula_feminino,
  COALESCE(quantidade_matricula_masculino, 0) AS qtd_matricula_masculino,
  COALESCE(quantidade_matricula_branca, 0) AS qtd_matricula_branca,
  COALESCE(quantidade_matricula_preta, 0) AS qtd_matricula_preta,
  COALESCE(quantidade_matricula_parda, 0) AS qtd_matricula_parda,
  COALESCE(quantidade_matricula_indigena, 0) AS qtd_matricula_indigena,
  COALESCE(quantidade_matricula_zona_residencia_rural, 0) AS qtd_matricula_zona_rural,
  COALESCE(quantidade_matricula_utiliza_transporte_publico, 0) AS qtd_matricula_transporte_publico,

  -- docentes e turmas (contexto — a versão agregada de docentes vem de
  -- outra fonte, ver silver_censo_docente_agregado.sql)
  COALESCE(quantidade_docente_fundamental_anos_finais, 0) AS qtd_docente_anos_finais,
  COALESCE(quantidade_docente_educacao_basica, 0) AS qtd_docente_educacao_basica,
  COALESCE(quantidade_turma_fundamental_anos_finais, 0) AS qtd_turma_anos_finais

FROM `basedosdados.br_inep_censo_escolar.escola`
WHERE id_escola IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola, ano ORDER BY id_escola) = 1;
