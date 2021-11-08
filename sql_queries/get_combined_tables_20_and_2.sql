SELECT
    *
FROM
    (SELECT
        *
    FROM
        interview.MICHELL_test_20 A UNION ALL SELECT
        *
    FROM
        interview.MICHELL_test_2 B) combined_tbls
