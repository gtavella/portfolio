/*
Q1 = committenti con pagamento confermato
Q2 = dipendenti di Q1 con i dati associati del committente del dipendente
Q3 = dipendenti Q2 ma solo quelli categorizzati come da pagare e con un stipendio effettivo > 0
Q4 = dipendenti Q3 meno quelli che sono (nelle N tabelle temporanee di) malattia
Q5 = dipendenti Q4 con i dati associati della societa' con cui e' stato fatturato il committente
Q_OUT = Q2
*/

WITH Q1 AS (
    SELECT 
        id_committente,
        progressivo_nel_mese
    FROM
        conferme
    WHERE
        anno = :anno
        AND mese = :mese
        AND conferma = 'PAGAMENTO'
        AND progressivo_nel_mese >= 1
),

Q2 AS (
    SELECT 
        B.id_committente 
            AS id_committente,
        C.ragione_sociale_appaltante 
            AS committente,
        B.progressivo_nel_mese 
            AS progressivo_nel_mese,
        A.id 
            AS id_dipendente,
        CONCAT(A.nome_dipendente, ' ', A.cognome_dipendente) 
            AS nome_dipendente,
        A.iban 
            AS iban
    FROM
        employee A
    JOIN 
        Q1 B
        ON B.id_committente = A.id_committente
    JOIN 
        contracts C
        ON C.id = A.id_committente
),

Q3 AS (
    SELECT 
        B.*,
        CAST(A.stipendio_effettivo AS FLOAT) 
            AS stipendio_effettivo,
        CAST(A.valore_malattia AS FLOAT) 
            AS valore_malattia,
        A.id_societa_fatturato 
            AS id_societa_fatturato
    FROM
        dipendenti_da_pagare A
    JOIN
        Q2 B
        ON B.id_dipendente = A.id_dipendente
        AND B.progressivo_nel_mese = A.numero_in_cartella
    WHERE
        A.anno = :anno
        AND A.mese = :mese
        AND A.stipendio_effettivo > 0
),

Q4 AS (
    SELECT *
    FROM 
        Q3 A
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM
                malattia_temp2
            WHERE
                anno = :anno
                AND mese = :mese
                AND id_dipendente = A.id_dipendente
                AND numero_in_cartella = A.progressivo_nel_mese
        )
        AND NOT EXISTS (
            SELECT 1
            FROM
                malattia_temp_soc2
            WHERE
                anno = :anno
                AND mese = :mese
                AND id_dipendente = A.id_dipendente
                AND numero_in_cartella = A.progressivo_nel_mese
        )
        -- .. aggiungi qui la malattia temp di un'altra societa' ..
),

Q5 AS (
    SELECT 
        A.*,
        B.ragione_sociale AS societa_fatturato
    FROM
        Q4 A
    JOIN
        societa B
        ON B.id = A.id_societa_fatturato
),


Q_OUT AS (
    SELECT *
    FROM
        Q5
),

Q_OUT_ORDERED AS (
    SELECT *
    FROM
        Q_OUT
    ORDER BY
        id_dipendente ASC,
        progressivo_nel_mese ASC
)


SELECT *
FROM 
    Q_OUT_ORDERED


