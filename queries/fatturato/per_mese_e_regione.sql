WITH comm_attivi_reg AS (
    SELECT 
        committ.id AS id_committente,
        mregioni.regione AS regione,
        mregioni.nome AS macroregione
    FROM 
        contracts committ
    INNER JOIN
        city_list citta        
        -- trova la regione che corrisponde a questa provincia
        ON committ.provincia_sede_legale_appaltante = citta.sigla
    INNER JOIN
        macroregioni mregioni
        -- trova la macroregione che corrisponde a questa regione
        ON citta.regione_nome = mregioni.regione
    WHERE
        -- seleziona questo committente in/per questa macroregione, 
        -- solo se e' anche un committente attivo in questo anno-mese
        committ.id IN (
            SELECT id
                    FROM (
                        -- questa query trova tutti i committenti attivi
                        -- in questo anno-mese
                        SELECT
                            comm.id AS id,
                            -- numero di dipendenti attivi, non mi interessa
                            COUNT(*)
                        FROM 
                            contracts comm
                        INNER JOIN
                            employee dip ON comm.id = dip.id_committente
                        WHERE
                            dip.inizio_contratto <= ?
                            AND dip.fine_contratto >= ?
                        GROUP BY 
                            comm.id
                    ) AS comm_attivi
        )
    -- raggruppa per committente e macroregione
    GROUP BY 
        committ.id,
        mregioni.regione,
        mregioni.nome
    -- questi sono tutti i committenti attivi per questo anno-mese,
    -- con accanto la propria regione e macroregione
),



totali_per_reg AS (
        SELECT 
                comm_reg.regione AS regione,
                comm_reg.macroregione AS macroregione,
                (SUM(COALESCE(fatt2.totale_gestione, 0))
                ) AS totali_gestione,
                (SUM(COALESCE(fatt2.totale_iva, 0))
                ) AS totali_iva,
                (SUM(COALESCE(fatt2.totale_stipendio, 0))
                ) AS totali_stipendio
        FROM
                comm_attivi_reg comm_reg
        INNER JOIN      
                fatture2_view fatt2
                ON fatt2.id_committente = comm_reg.id_committente
        WHERE 
                fatt2.anno = ?
                AND fatt2.mese = ?
        GROUP BY
                comm_reg.regione,
                comm_reg.macroregione   
),


comm_attivi_per_reg AS (
        SELECT 
                regione,
                macroregione,
                COUNT(*) AS tot
        FROM
                comm_attivi_reg
        GROUP BY
                regione,
                macroregione                
)


/*
il risultato finale deve essere:
colonne:
regione
macroregione
tot_committenti_attivi
totali_gestione
totali_iva
totali_stipendio
totali_gestione_e_iva
*/

SELECT 
        mreg.regione AS regione,
        mreg.nome AS macroregione,
        
        CAST((
            COALESCE(comm_reg.tot, 0) 
        ) AS FLOAT) AS tot_committenti_attivi,

        CAST(( 
            COALESCE(tot_reg.totali_gestione, 0)
        ) AS FLOAT) AS totali_gestione,

        CAST((
            COALESCE(tot_reg.totali_iva, 0) 
        ) AS FLOAT) AS totali_iva,

        CAST((
            COALESCE(tot_reg.totali_stipendio, 0) 
        ) AS FLOAT) AS totali_stipendio,

        CAST((
            COALESCE(tot_reg.totali_gestione, 0) 
            + COALESCE(tot_reg.totali_iva, 0)
        ) AS FLOAT) AS totali_gestione_e_iva

FROM 
        comm_attivi_per_reg comm_reg
-- prendi tutti i totali dei committenti attivi per macroregione
-- anche se non esistono/non ci sono totali per quelle macroregioni
LEFT JOIN
        totali_per_reg tot_reg
        ON comm_reg.regione = tot_reg.regione
-- prendi tutte le regioni, anche quelle che non hanno 
-- un committente attivo, e si interpeta che i suoi 
-- committenti attivi e totali sono 0
RIGHT JOIN
        macroregioni mreg
        ON comm_reg.regione = mreg.regione
ORDER BY 
    -- prima il (totale gestione + totale iva) piu' grande
    7 DESC,
    -- prima il totale committenti attivi piu' grande
    3 DESC        
