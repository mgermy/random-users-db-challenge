SELECT
    *
FROM
    (SELECT
        *
    FROM
        interview.MICHELL_test_20 A UNION SELECT
        *
    FROM
        interview.MICHELL_test_5 B) combined_tbls
