WITH confermato_presenza_SUBQ AS (
    SELECT 
    (EXISTS (
        SELECT *
        FROM 
            conferme
        WHERE
            id_committente = :id_committente
            AND anno = :anno
            AND mese = :mese
            AND conferma = 'PRESENZA'
            -- e' necessario esplicitare la logica del -1 
            -- perche' e' stato definito che 
            -- il progressivo nel mese = -1 significa
            -- "sono interessato al mese e non al progressivo nel mese" 
            AND progressivo_nel_mese = -1
    )) AS confermato
),

confermato_fattura_SUBQ AS (
    SELECT 
    (EXISTS (
        SELECT *
        FROM 
            conferme
        WHERE
            id_committente = :id_committente
            AND anno = :anno
            AND mese = :mese
            AND conferma = 'FATTURA'
            -- e' necessario esplicitare la logica del -1 
            -- perche' e' stato definito che 
            -- il progressivo nel mese = -1 significa
            -- "sono interessato al mese e non al progressivo nel mese" 
            AND progressivo_nel_mese = -1
    )) AS confermato
)



SELECT 
    (SELECT confermato FROM confermato_presenza_SUBQ) AS confermato_presenza,
    (SELECT confermato FROM confermato_fattura_SUBQ) AS confermato_fattura
