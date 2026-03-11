{{ config(materialized='table', tags=['mart']) }}

WITH stg AS (
    SELECT
        GEOGRAPHIC_ID,
        DATASET_ID,
        ATC1,
        ATC2,
        ATC3,
        ATC4
    FROM {{ ref('stg_iqvia_midas') }}
    WHERE ATC4 IS NOT NULL
),

final AS (
    SELECT DISTINCT
        GEOGRAPHIC_ID AS CNTRY_CD,
        DATASET_ID AS SRC,
        ATC1 AS ATC_1,
        ATC2 AS ATC_2,
        ATC3 AS ATC_3,
        ATC4 AS ATC_4,
        ATC4 AS CHLD_CD,
        '4' AS CHLD_LVL,
        ATC3 AS PRNT_CD,
        '3' AS PRNT_LVL,
        'ATC' AS TYP,
        ATC4 AS ATC4_VALUE
    FROM stg
)

SELECT
    CNTRY_CD,
    SRC,
    ATC_1,
    ATC_2,
    ATC_3,
    ATC_4,
    CHLD_CD,
    CHLD_LVL,
    PRNT_CD,
    PRNT_LVL,
    TYP,
    ATC4_VALUE
FROM final
