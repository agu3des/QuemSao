WITH
curso_2014 AS (
    SELECT *
    FROM public."2014_microdados2014_arq1"
    WHERE "CO_GRUPO"::int IN (72, 73, 76, 79, 4004, 4005, 4006, 5809, 5814)
      AND "CO_UF_CURSO"::int = 25
),
curso_2017 AS (
    SELECT *
    FROM public."2017_microdados2017_arq1"
    WHERE "CO_GRUPO"::int IN (72, 73, 76, 79, 4004, 4005, 4006, 5809, 5814)
      AND "CO_UF_CURSO"::int = 25
),
curso_2021 AS (
    SELECT *
    FROM public."2021_microdados2021_arq1"
    WHERE "CO_GRUPO"::int IN (72, 73, 76, 79, 4004, 4005, 4006, 5809, 5814)
      AND "CO_UF_CURSO"::int = 25
)

SELECT
    c."NU_ANO",
    c."CO_CURSO",
    c."CO_IES",
    c."CO_CATEGAD",
    c."CO_ORGACAD",
    c."CO_GRUPO",
    c."CO_MODALIDADE",
    c."CO_MUNIC_CURSO",
    c."CO_UF_CURSO",
    c."CO_REGIAO_CURSO",
    b."ANO_FIM_EM",
    b."ANO_IN_GRAD",
    b."IN_MATUT",
    b."IN_VESPER",
    b."IN_NOTURNO",
    s."TP_SEXO",
    i."NU_IDADE",
    q."QE_I02" AS "RACA_COR",
    j."QE_I03" AS "NACIONALIDADE",
    k."QE_I06" AS "QUEM_MORA",
    l."QE_I08" AS "RENDA",
    m."QE_I09" AS "SITU_FINAC",
    n."QE_I10" AS "SITU_TRAB",
    o."QE_I12" AS "BOLSA_PERM",
    p."QE_I15" AS "POL_AFIRM",
    r."QE_I17" AS "TP_MEDIO",
    2014 AS "ANO"
FROM curso_2014 c
LEFT JOIN public."2014_microdados2014_arq2" b ON c."CO_CURSO"::text = b."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq5" s ON c."CO_CURSO"::text = s."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq6" i ON c."CO_CURSO"::text = i."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq8" q ON c."CO_CURSO"::text = q."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq9" j ON c."CO_CURSO"::text = j."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq12" k ON c."CO_CURSO"::text = k."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq14" l ON c."CO_CURSO"::text = l."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq15" m ON c."CO_CURSO"::text = m."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq16" n ON c."CO_CURSO"::text = n."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq18" o ON c."CO_CURSO"::text = o."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq21" p ON c."CO_CURSO"::text = p."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq23" r ON c."CO_CURSO"::text = r."CO_CURSO"

UNION ALL

SELECT
    c."NU_ANO",
    c."CO_CURSO",
    c."CO_IES",
    c."CO_CATEGAD",
    c."CO_ORGACAD",
    c."CO_GRUPO",
    c."CO_MODALIDADE",
    c."CO_MUNIC_CURSO",
    c."CO_UF_CURSO",
    c."CO_REGIAO_CURSO",
    b."ANO_FIM_EM",
    b."ANO_IN_GRAD",
    b."IN_MATUT",
    b."IN_VESPER",
    b."IN_NOTURNO",
    s."TP_SEXO",
    i."NU_IDADE",
    q."QE_I02" AS "RACA_COR",
    j."QE_I03" AS "NACIONALIDADE",
    k."QE_I06" AS "QUEM_MORA",
    l."QE_I08" AS "RENDA",
    m."QE_I09",
    n."QE_I10",
    o."QE_I12",
    p."QE_I15",
    r."QE_I17",
    2017 AS "ANO"
FROM curso_2017 c
LEFT JOIN public."2017_microdados2017_arq2" b ON c."CO_CURSO"::text = b."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq5" s ON c."CO_CURSO"::text = s."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq6" i ON c."CO_CURSO"::text = i."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq8" q ON c."CO_CURSO"::text = q."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq9" j ON c."CO_CURSO"::text = j."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq12" k ON c."CO_CURSO"::text = k."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq14" l ON c."CO_CURSO"::text = l."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq15" m ON c."CO_CURSO"::text = m."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq16" n ON c."CO_CURSO"::text = n."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq18" o ON c."CO_CURSO"::text = o."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq21" p ON c."CO_CURSO"::text = p."CO_CURSO"
LEFT JOIN public."2017_microdados2017_arq23" r ON c."CO_CURSO"::text = r."CO_CURSO"

UNION ALL

SELECT
    c."NU_ANO",
    c."CO_CURSO",
    c."CO_IES",
    c."CO_CATEGAD",
    c."CO_ORGACAD",
    c."CO_GRUPO",
    c."CO_MODALIDADE",
    c."CO_MUNIC_CURSO",
    c."CO_UF_CURSO",
    c."CO_REGIAO_CURSO",
    b."ANO_FIM_EM",
    b."ANO_IN_GRAD",
    b."IN_MATUT",
    b."IN_VESPER",
    b."IN_NOTURNO",
    s."TP_SEXO",
    i."NU_IDADE",
    q."QE_I02" AS "RACA_COR",
    j."QE_I03" AS "NACIONALIDADE",
    k."QE_I06" AS "QUEM_MORA",
    l."QE_I08" AS "RENDA",
    m."QE_I09",
    n."QE_I10",
    o."QE_I12",
    p."QE_I15",
    r."QE_I17",
    2021 AS "ANO"
FROM curso_2021 c
LEFT JOIN public."2021_microdados2021_arq2" b ON c."CO_CURSO"::text = b."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq5" s ON c."CO_CURSO"::text = s."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq6" i ON c."CO_CURSO"::text = i."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq8" q ON c."CO_CURSO"::text = q."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq9" j ON c."CO_CURSO"::text = j."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq12" k ON c."CO_CURSO"::text = k."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq14" l ON c."CO_CURSO"::text = l."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq15" m ON c."CO_CURSO"::text = m."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq16" n ON c."CO_CURSO"::text = n."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq18" o ON c."CO_CURSO"::text = o."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq21" p ON c."CO_CURSO"::text = p."CO_CURSO"
LEFT JOIN public."2021_microdados2021_arq23" r ON c."CO_CURSO"::text = r."CO_CURSO";



/*with curso as (
    select 
        a."NU_ANO",
        a."CO_CURSO",
        a."CO_IES",
        a."CO_CATEGAD",
        a."CO_ORGACAD",
        a."CO_GRUPO",
        a."CO_MODALIDADE",
        a."CO_MUNIC_CURSO",
        a."CO_UF_CURSO",
        a."CO_REGIAO_CURSO" 
     FROM public."2014_microdados2014_arq1" a
    WHERE a."CO_GRUPO"::int IN (72, 73, 76, 79, 4004, 4005, 4006, 5809, 5814)
      AND a."CO_UF_CURSO"::int = 25

)
SELECT 
    c."NU_ANO",
    c."CO_CURSO",
    c."CO_IES",
    c."CO_CATEGAD",
    c."CO_ORGACAD",
    c."CO_GRUPO",
    c."CO_MODALIDADE",
    c."CO_MUNIC_CURSO",
    c."CO_UF_CURSO",
    c."CO_REGIAO_CURSO", 
    b."ANO_FIM_EM",
    b."ANO_IN_GRAD",
    b."IN_MATUT",
    b."IN_VESPER",
    b."IN_NOTURNO",
    s."TP_SEXO",
    i."NU_IDADE",
    q."QE_I02" AS "RACA_COR",
    j."QE_I03" AS "NACIONALIDADE",
    k."QE_I06" AS "QUEM_MORA",
    l."QE_I08" AS "RENDA"
FROM curso c
LEFT JOIN public."2014_microdados2014_arq2" b
    ON c."CO_CURSO"::text = b."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq5" s
    ON c."CO_CURSO"::text = s."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq6" i
    ON c."CO_CURSO"::text = i."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq8" q
    ON c."CO_CURSO"::text = q."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq9" j
    ON c."CO_CURSO"::text = j."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq12" k
    ON c."CO_CURSO"::text = k."CO_CURSO"
LEFT JOIN public."2014_microdados2014_arq14" l
    ON c."CO_CURSO"::text = l."CO_CURSO";
    




-- Criar a base com o formato em latin
CREATE DATABASE enade
WITH ENCODING 'LATIN1'
LC_COLLATE='C'
LC_CTYPE='C'
TEMPLATE=template0;


-- Para a tabela arq1
CREATE INDEX idx_arq1_co_curso
ON public."2014_microdados2014_arq1"("CO_CURSO");

-- Para a tabela arq5
CREATE INDEX idx_arq5_co_curso
ON public."2014_microdados2014_arq5"("CO_CURSO");

-- Para a tabela arq6
CREATE INDEX idx_arq6_co_curso
ON public."2014_microdados2014_arq6"("CO_CURSO");

-- Para a tabela arq8
CREATE INDEX idx_arq8_co_curso
ON public."2014_microdados2014_arq8"("CO_CURSO");

-- Para a tabela arq9
CREATE INDEX idx_arq9_co_curso
ON public."2014_microdados2014_arq9"("CO_CURSO");

-- Para a tabela arq12
CREATE INDEX idx_arq12_co_curso
ON public."2014_microdados2014_arq12"("CO_CURSO");

-- Para a tabela arq14
CREATE INDEX idx_arq14_co_curso
ON public."2014_microdados2014_arq14"("CO_CURSO");
*/