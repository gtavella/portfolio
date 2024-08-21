
--  end date is:
--    if anno esercizio >= current year:
--        then today
--    else:
--        last day of anno esercizio        
WITH end_date AS (
    SELECT 
        CASE 
            WHEN {anno_esercizio_placehold} >= EXTRACT(YEAR FROM CURRENT_DATE) THEN CURRENT_DATE
            ELSE MAKE_DATE({anno_esercizio_placehold}, 12, 31)
        END AS d
), 
    
-- in with statement
-- these conti correnti are 'validated', 
-- in the sense that the error is null only if
-- there was no error, else it's a text/string containing
-- informal description of the error
-- when the error is null, it's interpreted as "this conto 
-- corrente is valid, and thus you can safely calculate the saldo"

valid_cc AS (
    SELECT 
        cc.id AS id,
        cc.nome_banca AS nome_banca,
        cc.numero as numero_conto,
        cc.data_saldo_iniziale AS data_saldo_iniziale,
        cc.saldo_iniziale AS saldo_iniziale,
        cc.created_at AS created_at,
        CASE 
            WHEN cc.saldo_iniziale IS NULL THEN 'saldo iniziale nullo'
            WHEN cc.data_saldo_iniziale IS NULL THEN 'data saldo iniziale nulla'
            WHEN cc.data_saldo_iniziale > CURRENT_DATE THEN 'data saldo iniziale non valida'
            WHEN {anno_esercizio_placehold} < EXTRACT(YEAR FROM cc.data_saldo_iniziale) THEN 'non disp.'
            ELSE NULL
        END AS err
    FROM conti_correnti cc
    WHERE cc.id_ass = {id_ass_placehold}
    -- here go the conditions/filters that select 
    -- the conti correnti whose saldo will be computed 
    -- because this is a where clause 
    {conti_correnti_cond}
),
    
-- in with statement
-- this calculation does not perform any validation or check:
-- it's assumed that when it's called, the conto corrente is valid

compute_diff AS (
    SELECT 
        vc.id,
        vc.nome_banca,
        vc.numero_conto,
        vc.data_saldo_iniziale,
        vc.saldo_iniziale,
        vc.created_at,
        SUM(CASE
                WHEN mv.tipo_movimento = 1 AND mv.pagato = TRUE 
                    THEN COALESCE(mv.importo, 0) * COALESCE(mv.quantita, 0)
                WHEN mv.tipo_movimento = 0 
                    THEN -COALESCE(mv.importo, 0) * COALESCE(mv.quantita, 0)
                ELSE 0
            END) AS diff
    -- get all conti correnti: the valid conti correnti
    -- will be identified/marked by the error being null
    FROM valid_cc vc
    -- the movimenti
    LEFT JOIN movimenti mv
        ON mv.id_conto_corrente = vc.id
        -- only select movimenti made with conto corrente 
        -- it's a further guarantee that you're not accidentally 
        -- selecting movimenti made without conto corrente
        AND mv.metodo_pagamento = 1
        -- filter out movimenti that do not fit the time range
        -- time range being:
        -- [data saldo iniziale: [date end of anno esercizio|today]] 
        AND mv.data_pagamento BETWEEN vc.data_saldo_iniziale AND (SELECT d FROM end_date)
    -- compute diff only for valid conti correnti
    WHERE vc.err IS NULL
    GROUP BY vc.id,  
            vc.nome_banca, 
            vc.numero_conto,
            vc.data_saldo_iniziale, 
            vc.saldo_iniziale,
            vc.created_at
)

-- final query
SELECT 
    vc.id, 
    vc.nome_banca,
    vc.numero_conto,
    vc.data_saldo_iniziale,
    vc.saldo_iniziale,
    vc.created_at, 
    -- since it's a left join, this value will be null if there was an error
    cd.diff AS diff,
    (CASE WHEN cd.saldo_iniziale IS NULL OR cd.diff IS NULL THEN NULL 
        ELSE vc.saldo_iniziale + cd.diff END) AS saldo,
    vc.err
FROM valid_cc vc
LEFT JOIN compute_diff cd ON vc.id = cd.id
