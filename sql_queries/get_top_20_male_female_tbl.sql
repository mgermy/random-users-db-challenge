SELECT
    *
FROM
    (SELECT
        *
    FROM
        interview.MICHELL_test_female
    ORDER BY REGISTERED_DATE DESC
    LIMIT 20) TOP_20_FEMALE
UNION ALL SELECT
    *
FROM
    (SELECT
        *
    FROM
        interview.MICHELL_test_male
    ORDER BY REGISTERED_DATE DESC
    LIMIT 20) TOP_20_MALE;
