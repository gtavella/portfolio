WITH committ AS (
    SELECT 
           contr.id AS id_committente,
           contr.ragione_sociale_appaltante AS committente, 
           COUNT(*) AS tot_dipendenti_attivi
    FROM 
        contracts contr
    JOIN 
        employee empl
        ON empl.id_committente = contr.id
    WHERE 
        contr.commerciale = ?
        AND empl.inizio_contratto <= '{$fine_mese}'
        AND empl.fine_contratto >= '{$inizio_mese}'
    GROUP BY
        id_committente
),

-- this CTE builds upon the previous one
provvigione_calculations AS (
    SELECT
        committ.id_committente AS id_committente,  
        committ.committente AS committente,
        committ.tot_dipendenti_attivi AS tot_dipendenti_attivi,

        -- totale gestione
        COALESCE((
            SELECT 
                COALESCE(fatt2.totale_gestione, 0) 
            FROM 
                fatture2_view fatt2
            WHERE 
                fatt2.id_committente = committ.id_committente
                AND fatt2.anno = {$anno}
                AND fatt2.mese = {$mese}
        ), 0) AS totale_gestione,

        -- provvigione temporanea
        (0.05 * COALESCE((
            SELECT 
                COALESCE(fatt2.totale_gestione, 0) 
            FROM 
                fatture2_view fatt2
            WHERE 
                fatt2.id_committente = committ.id_committente
                AND fatt2.anno = {$anno}
                AND fatt2.mese = {$mese}
        ), 0)) AS calculated_provvigione
    FROM 
        committ
)

SELECT
       provv.id_committente AS id_committente,
       provv.committente AS committente,
       provv.tot_dipendenti_attivi AS tot_dipendenti_attivi,

       -- provvigione
        (CASE
            WHEN provv.tot_dipendenti_attivi = 1 THEN 50
            WHEN provv.tot_dipendenti_attivi IN (2, 3) THEN 60
            WHEN provv.tot_dipendenti_attivi IN (4, 5) THEN 70
            WHEN provv.tot_dipendenti_attivi >= 6 THEN 
                (CASE 
                    WHEN provv.calculated_provvigione = 0 THEN 0
                    WHEN provv.calculated_provvigione BETWEEN 1 AND 70 THEN 70
                    ELSE provv.calculated_provvigione
                END)
            ELSE 0
        END) AS provvigione, 

       -- inizio condizione commerciale riceve provvigione  
       -- per questo committente solo se: totale gestione > 0 AND pagato=si
       (
            -- totale gestione > 0 ?
            EXISTS (SELECT *
                    FROM 
                        fatture2_view fatt2
                    WHERE
                        fatt2.id_committente = provv.id_committente
                        AND fatt2.anno = {$anno}
                        AND fatt2.mese = {$mese}
                        AND fatt2.totale_gestione > 0)
            AND
            -- pagato = si ?
            EXISTS (SELECT *
                    FROM 
                        conferme_mensili_per_mese_view conf
                    WHERE
                        conf.id_committente = provv.id_committente
                        AND conf.anno = {$anno}
                        AND conf.mese = {$mese}
                        AND conf.pagamento = 1)
        ) AS cond_provvigione,

        -- pagato
        (
            EXISTS (SELECT *
                    FROM 
                        conferme_mensili_per_mese_view conf
                    WHERE
                        conf.id_committente = provv.id_committente
                        AND conf.anno = {$anno}
                        AND conf.mese = {$mese}
                        AND conf.pagamento = 1)
        ) AS pagato,

        (
            -- totale gestione = 0 ?
            NOT EXISTS (SELECT *
                    FROM 
                        fatture2_view fatt2
                    WHERE
                        fatt2.id_committente = provv.id_committente
                        AND fatt2.anno = {$anno}
                        AND fatt2.mese = {$mese}
                        AND fatt2.totale_gestione > 0)
            AND
            -- pagato = si ?
            EXISTS (SELECT *
                    FROM 
                        conferme_mensili_per_mese_view conf
                    WHERE
                        conf.id_committente = provv.id_committente
                        AND conf.anno = {$anno}
                        AND conf.mese = {$mese}
                        AND conf.pagamento = 1)
        ) AS non_fatturato

-- committenti attivi di questo commerciale
FROM 
    provvigione_calculations provv
JOIN
    committ 
    ON provv.id_committente = committ.id_committente
ORDER BY 
    provv.tot_dipendenti_attivi DESC, 
    provv.committente ASC;
