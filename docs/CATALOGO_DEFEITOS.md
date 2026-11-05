# Catálogo de defeitos e regras de limpeza

Este catálogo liga três coisas: o problema que existe na bronze, como ele aparece na mensagem JSON que chega pelo Pub/Sub e a regra que o pipeline aplica. Os mesmos códigos aparecem na coluna `regras_aplicadas` da tabela `auditoria_limpeza_stream` e no arquivo `dados/gabarito/manifesto_defeitos.csv`. Assim, durante a demonstração, dá para apontar uma linha suja no arquivo e mostrar a decisão tomada sobre ela no BigQuery.

## Como ler o destino de cada regra

Cada mensagem termina em um de três destinos. **Aceito** quer dizer que o registro foi limpo e gravado na silver de streaming; quando a regra recupera o valor original, o dado limpo fica idêntico ao real. **Descartado** é a aplicação de um filtro de negócio herdado da fase 1, como etapa fora do recorte ou IDEB suprimido: não se trata de erro, e sim do mesmo corte que a silver em lote faz. **Quarentena** é um erro de qualidade que impede o uso do registro (chave ausente, valor fora de qualquer escala plausível, JSON corrompido); o payload cru fica guardado na auditoria para inspeção.

Há também regras que só **anulam um campo**: o registro é aceito, mas um valor impossível vira nulo, porque não existe como recuperar o número verdadeiro. Isso acontece com indicadores fora da faixa e idade implausível de docente.

A coluna "Fase" indica se a regra reproduz exatamente uma decisão da fase 1 ("fase 1") ou se é nova ("nova"). As regras novas existem porque, no streaming, o dado chega cru, em JSON, com problemas de tipo e formatação que o BigQuery resolvia sozinho quando a bronze era uma tabela tipada.

## Regras com estado (rodam na consolidação, não no limpador)

O limpador trata cada mensagem isoladamente, sem memória. Três regras da fase 1 dependem de olhar várias linhas ao mesmo tempo e, por isso, rodam nas views de consolidação, a cada micro-lote do construtor da gold:

| Código | Regra | Onde |
|---|---|---|
| G01 | Reenvio do mesmo registro (o Pub/Sub entrega "pelo menos uma vez" e a origem reenvia) | Views `vw_silver_stream_*`: fica a versão mais recente por chave |
| IDB06 | Duas linhas de IDEB para a mesma escola e ano, com valores diferentes | `vw_silver_stream_ideb_escola`: fica o maior IDEB (regra da fase 1) |
| INS03 | Mais de um INSE para a mesma escola e ano | `vw_silver_stream_inse_escola`: fica o calculado com mais alunos (regra da fase 1) |
| DOC03 | O mesmo vínculo docente x turma recebido duas vezes | `vw_silver_stream_censo_docente_agregado`: conta uma vez só |
| (agregação) | Grão docente x turma para grão escola x ano | `vw_silver_stream_censo_docente_agregado`: COUNT(DISTINCT id_docente) com filtros |
| (junção) | As cinco fontes da mesma escola chegam em momentos diferentes | MERGE da gold: só entra escola completa e estável |

## Regras genéricas (todas as tabelas)

| Código | Problema na bronze | Como chega no JSON | Regra | Destino | Fase | Ocorrências* |
|---|---|---|---|---|---|---|
| G02 | `id_escola` com formatação suja | número em vez de texto, espaços, `35.990.001`, tabulação | mantém só os dígitos e exige 8 | aceito | nova | 703 |
| G03 | `id_escola` ausente ou inválido | campo ausente, vazio, `"NA"` | descarta a linha | quarentena | fase 1 (`id_escola IS NOT NULL`) | 144 |
| G04 | `ano` em formato errado | `"2023"`, `2023.0`, `" 2023 "` | converte para inteiro; fora de 2005 a 2030 vai para quarentena | aceito | nova | 606 |
| G05 | `sigla_uf` em caixa ou espaços errados | `"sp"`, `" SP "` | maiúsculas sem espaços; UF inexistente vai para quarentena | aceito | nova | 63 |
| G06 | JSON corrompido (mensagem truncada) | `{"ano": 2023, "sigla_uf": "PA", "i` | não há o que limpar | quarentena | nova | 118 |
| G07 | Campo inesperado (mudança de layout na origem) | `"versao_layout": "layout-2024"` | ignora o campo; a projeção mantém só as colunas da silver | aceito | nova | 133 |
| G08 | Número enviado como texto | `"269"`, `"269.0"` | converte | aceito | nova | 72 |

## IDEB (`bronze_ideb_escola`, 14 colunas)

| Código | Problema na bronze | Como chega no JSON | Regra | Destino | Fase | Ocorrências* |
|---|---|---|---|---|---|---|
| IDB01 | Linhas de outras etapas (anos iniciais, ensino médio) | `"anos_escolares": "iniciais (1-5)"` | fica só `fundamental` + `finais (6-9)` | descartado | fase 1 | 2.077 |
| IDB02 | Etapa escrita com caixa ou espaços diferentes | `"Fundamental"`, `"FINAIS (6-9) "` | normaliza o texto antes do filtro (sem isso a escola seria perdida) | aceito | nova | 115 |
| IDB03 | IDEB suprimido pelo INEP (participação insuficiente) | ausente, `"-"`, `"*"`, `""` | descarta, como na fase 1 | descartado | fase 1 | 69 |
| IDB04 | Vírgula decimal | `"4,7"` | troca por ponto | aceito | nova | 78 |
| IDB05 | IDEB fora da escala (vírgula perdida na digitação) | `47.0` | fora de 0 a 10 | quarentena | nova | 16 |
| IDB06 | Duplicata conflitante | segunda linha com IDEB menor | consolidação: maior IDEB | aceito | fase 1 | 27 |
| IDB07 | Componentes da fórmula do IDEB | `taxa_aprovacao`, notas SAEB, `indicador_rendimento`, `projecao` | projeção remove (evita vazamento) | aceito | fase 1 | todas |

## Censo Escola (`bronze_censo_escola`, 455 colunas, 64 vão para a silver)

| Código | Problema na bronze | Como chega no JSON | Regra | Destino | Fase | Ocorrências* |
|---|---|---|---|---|---|---|
| CEN01 | Atributo nulo | campo ausente ou `""` | COALESCE para 0 ("a escola não possui o item") | aceito | fase 1 | 2.029 linhas** |
| CEN02 | Flag escrita como texto ou booleano | `"Sim"`, `"N"`, `"true"`, `true`, `"1.0"` | converte para 0/1 | aceito | nova | 171 |
| CEN03 | Sentinela de "não informado" | `-1`, `"-1"` | trata como nulo e aplica o COALESCE | aceito | nova | 80 |
| CEN04 | Categoria fora do código usado no treino | `"Municipal"`, `"URBANA"`, `"3.0"`, `0` em `tipo_localizacao_diferenciada` | converte para o código exato do treino (`"3"`, `"1"`, `"0.0"`); valor desconhecido vai para quarentena | aceito | nova | 150 |
| CEN05 | Quantidade em texto | `"1.234"` (milhar), `"270.0"` | converte para inteiro | aceito | nova | 95 |
| CEN06 | `id_municipio` com formato errado | `3550308` (número), `"3550308.0"` | 7 dígitos com código de UF válido; senão quarentena | aceito | nova | 49 |
| CEN07 | 391 colunas fora do recorte | todas as demais | projeção para as 64 colunas da fase 1 | aceito | fase 1 | todas |

\*\* Em 2019, oito dos 57 atributos da silver chegam sempre nulos da Base dos Dados (por exemplo `quantidade_sala_existente`, `quantidade_computador` e `quantidade_funcionario`), então toda linha do censo passa pelo CEN01. Além disso, o gerador anula de propósito alguns campos cujo valor real é 0, para mostrar a regra recuperando o valor certo.

## Indicadores (`bronze_indicadores_escola`, 208 colunas, 13 vão para a silver)

| Código | Problema na bronze | Como chega no JSON | Regra | Destino | Fase | Ocorrências* |
|---|---|---|---|---|---|---|
| IND01 | Marcador de "não se aplica" | `"--"`, `"-"`, `""` | vira nulo (a fase 1 não aplica COALESCE nos indicadores) | aceito | nova | 15 |
| IND02 | Vírgula decimal | `"26,5"` | troca por ponto | aceito | nova | 169 |
| IND03 | Percentual com símbolo | `"63,8%"`, `"63.8%"` | remove o símbolo e converte | aceito | nova | 85 |
| IND04 | Complexidade de gestão com grafia diferente | `"Nível 3"`, `"NIVEL 3"`, `"nivel_3"`, `3` | converte para `"nivel 3"` (domínio do treino) | aceito | nova | 152 |
| IND05 | Valor fora da faixa do indicador | AFD `158.3`, média de alunos por turma negativa, `99` horas-aula | campo anulado; o resto do registro é aproveitado | aceito (campo nulo) | nova | 25 |

## INSE (`bronze_inse_escola`, 18 colunas)

| Código | Problema na bronze | Como chega no JSON | Regra | Destino | Fase | Ocorrências* |
|---|---|---|---|---|---|---|
| INS01 | INSE nulo | ausente, `""`, `"-"` | descarta, como na fase 1 | descartado | fase 1 | 55 |
| INS02 | Vírgula decimal | `"4,72"` | troca por ponto | aceito | nova | 164 |
| INS03 | Dois INSE para a mesma escola | linha extra com menos alunos | consolidação: maior `quantidade_alunos_inse` | aceito | fase 1 | 43 |
| INS04 | INSE fora da escala (escala antiga ou erro) | `47.2` | fora de 1 a 9 | quarentena | nova | 9 |

## Docente (`bronze_censo_docente`, 126 colunas, grão docente x turma)

| Código | Problema na bronze | Como chega no JSON | Regra | Destino | Fase | Ocorrências* |
|---|---|---|---|---|---|---|
| DOC01 | Vínculo em outra etapa (infantil, anos iniciais, médio, EJA) | `"etapa_ensino": "27"` | fica só 6º ao 9º ano (`19`, `20`, `21`, `41`) | descartado | fase 1 | 8.990 |
| DOC02 | Vínculo sem etapa (atividade complementar, AEE) | campo ausente | descarta | descartado | fase 1 | 1.779 |
| DOC03 | Vínculo reenviado | mesma linha duas vezes | consolidação conta uma vez | aceito | fase 1 | 601 |
| DOC04 | Idade implausível | `2`, `150`, `999`, `-5`, `"abc"` | fora de 16 a 90 vira nulo e sai da média de idade | aceito (campo nulo) | nova | 479 |
| DOC05 | Escolaridade em texto | `"Superior Completo"`, `"superior"`, `"4.0"`, `4` | converte para o código do dicionário (`"4"`) | aceito | nova | 1.861 |
| DOC06 | Flags em texto | `"Sim"`, `"N"`, `"true"` | converte para 0/1 | aceito | nova | 1.764 |
| DOC07 | `id_docente` ausente | ausente ou `""` | sem ele não dá para contar docentes distintos | quarentena | nova | 125 |
| DOC08 | Tipo de contratação em texto | `"Concursado/efetivo/estável"`, `"temporario"`, `1` | converte para o código (`"1"` = efetivo) | aceito | nova | 1.282 |
| DOC09 | Etapa como número ou decimal | `19`, `"19.0"`, `" 19 "` | converte para o código em texto | aceito | nova | 1.196 |

\* Ocorrências no conjunto padrão (2.000 escolas, semente 42), contadas no manifesto e na auditoria do teste ponta a ponta.

## O que a agregação de docentes calcula

Os nomes e tipos são os da `silver_censo_docente_agregado` de produção, conferidos no DDL exportado. As expressões foram reconstruídas a partir das estatísticas da gold real: todas as contagens ficam no máximo iguais ao total de docentes da escola, o que só acontece com `COUNT(DISTINCT ...)`; a média de idade tem uma casa decimal.

| Coluna | Expressão (sobre os vínculos de 6º a 9º ano, já sem duplicatas) |
|---|---|
| `qtd_docente_anos_finais` | `COUNT(DISTINCT id_docente)` |
| `idade_media_docente` | `ROUND(AVG(idade), 1)` |
| `qtd_docente_escolaridade_superior` | docentes distintos com `escolaridade = '4'` |
| `qtd_docente_licenciatura` | docentes distintos com `licenciatura_1 = 1` |
| `qtd_docente_especializacao`, `_mestrado`, `_doutorado` | docentes distintos com a flag = 1 |
| `qtd_docente_efetivo` | docentes distintos com `tipo_contratacao = '1'` |
| `qtd_docente_tipo_docente_regular` | docentes distintos com `tipo_docente = 1` |
| `qtd_docente_disciplina_matematica`, `_lingua_portuguesa` | docentes distintos com a flag = 1 |
