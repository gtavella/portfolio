
WITH calc_end_date AS (
    SELECT 
        -- this is just a helper query
        CASE 
            -- if anno esercizio is the same or greater than the current year,
            -- then consider today as the end date
            WHEN {anno_eserc} >= EXTRACT(YEAR FROM CURRENT_DATE) THEN CURRENT_DATE
            -- otherwise the end date is the last of the year of anno esercizio
            ELSE MAKE_DATE({anno_eserc}, 12, 31)
        END AS end_date
), 
 

valid_cc AS (
    SELECT 
        -- in with statement
        -- these conti correnti are 'validated', 
        -- in the sense that the error is null only if
        -- there was no error, else it's a text/string containing
        -- informal description of the error
        -- when the error is null, it's interpreted as "this conto 
        -- corrente is valid, and thus you can safely calculate the saldo"
        cc.id AS id,
        cc.nome_banca AS nome_banca,
        cc.numero_conto as numero_conto,
        cc.iban AS iban,
        cc.virtuale AS virtuale,
        cc.saldo_iniziale AS saldo_iniziale,
        cc.data_saldo_iniziale AS data_saldo_iniziale,
        cc.agenzia AS agenzia,
        cc.created_at AS created_at,
        -- you want to calculate the saldo of the conti correnti
        -- who are "valid". the validity of a conto corrente
        -- is determined by the non-nullity of the err column of this virtual table
        -- in short, if the err column of this virtual table is anything 
        -- but null, then it means this column, at that row, contains 
        -- a message (string) indicating the feedback error about the non-validity
        -- of that specific conto corrente, specifying why it's not valid,
        -- and why the saldo of that conto corrente won't be computed
        CASE 
            WHEN cc.saldo_iniziale IS NULL 
                THEN 'saldo iniz. nullo'
            WHEN cc.data_saldo_iniziale IS NULL 
                THEN 'data saldo iniz. nulla'
            WHEN cc.data_saldo_iniziale > CURRENT_DATE 
                THEN 'data saldo iniz. non valida'
            -- if anno esercizio is less than the year of the data saldo iniziale
            -- of that specific conto corrente, there's no hope 
            -- the system can determine/calc what is the 
            -- saldo of that specific conto corrente   
            WHEN {anno_eserc} < EXTRACT(YEAR FROM cc.data_saldo_iniziale) 
                THEN 'non disp.'
            ELSE NULL
        END AS err
    FROM 
        conti_correnti cc
    WHERE 
        -- select the conti correnti of this associazione
        cc.id_ass = {id_ass}
        -- add more conditions if you want
        -- you must qualify the identifier with "cc"
        -- because it's the alias for conti correnti
        {which_conti}
),


compute_diff AS (
    SELECT         
        -- this calculation does not perform any validation or check:
        -- it's assumed that when it's called, each conto corrente is valid
        vc.id AS id,
        vc.nome_banca AS nome_banca,
        vc.numero_conto as numero_conto,
        vc.iban AS iban,
        vc.virtuale AS virtuale,
        vc.saldo_iniziale AS saldo_iniziale,
        vc.data_saldo_iniziale AS data_saldo_iniziale,
        vc.agenzia AS agenzia,
        vc.created_at AS created_at,
        SUM(CASE
                -- if this movimento is an entrata and
                -- the entrata is pagata, then consider it (add)
                WHEN mv.tipo_movimento = 'entrata' AND mv.pagato = TRUE 
                    THEN COALESCE(mv.importo, 0) * COALESCE(mv.quantita, 0)
                -- if this movimento is an uscita, consider it (subtract)
                WHEN mv.tipo_movimento = 'uscita'
                    THEN -COALESCE(mv.importo, 0) * COALESCE(mv.quantita, 0)
                -- otherwise sum 0
                ELSE 0
            END) AS diff
    -- get all conti correnti: the valid conti correnti
    -- will be identified/marked by the err column being null
    FROM 
        valid_cc vc
    -- output set has all conti correnti, even those that do not yet
    -- have a match with the following conditions, because
    -- this is a left join
    LEFT JOIN 
        movimenti mv
        ON mv.id_conto_corrente = vc.id
        -- only select movimenti made with conto corrente 
        AND mv.metodo_pagamento = 'conto corrente'
        -- filter out movimenti that do not fit the time range
        -- time range being:
        -- [data saldo iniziale until [last of anno esercizio|today]] 
        AND 
            mv.data_pagamento 
            BETWEEN vc.data_saldo_iniziale 
            AND (SELECT end_date FROM calc_end_date)
    -- compute diff only for valid conti correnti,
    -- so conti correnti whose err column is null
    WHERE 
        vc.err IS NULL
    GROUP BY vc.id,
            vc.nome_banca,
            vc.numero_conto,
            vc.iban,
            vc.virtuale,
            vc.saldo_iniziale,
            vc.data_saldo_iniziale,
            vc.agenzia,
            vc.created_at
)


SELECT -- this query is the final, resulting query that is run to calc saldi 
    vc.id AS id,
    vc.nome_banca AS nome_banca,
    vc.numero_conto as numero_conto,
    vc.iban AS iban,
    vc.virtuale AS virtuale,
    vc.saldo_iniziale AS saldo_iniziale,
    vc.data_saldo_iniziale AS data_saldo_iniziale,
    vc.agenzia AS agenzia,
    vc.created_at AS created_at,
    -- calculated/virtual columns have a leading underscore _
    -- to differentiate them between material/real columns
    vc.err AS _err,
    -- since it's a left join, this value will be null if there was an error
    cd.diff AS _diff,
    CASE 
        -- saldo will be null with these conditions
        WHEN cd.saldo_iniziale IS NULL OR cd.diff IS NULL 
            THEN NULL 
        -- this is where the saldo is finally calculated,
        -- it's simply the sum of saldo iniziale and the diff
        -- that was calculated between entrate and uscite
        -- in the time range
        ELSE vc.saldo_iniziale + cd.diff 
    END AS _saldo
FROM 
    valid_cc vc
LEFT JOIN 
    compute_diff cd 
    ON vc.id = cd.id

