SELECT
    committ_attivi.id AS id_committente,
    committ_attivi.committente AS committente,
    (COALESCE(fatt2.totale_gestione, 0)) AS totale_gestione,
    (COALESCE(fatt2.totale_iva, 0)) AS iva,
    conf.pagamento AS pagato,
    (COALESCE(fatt2.totale_gestione, 0) + COALESCE(fatt2.totale_iva, 0)) AS totale_gestione_e_iva
-- committenti attivi
FROM (
        SELECT
            committ.id AS id,
            committ.ragione_sociale_appaltante AS committente,
            -- totale dipendenti attivi, non mi interessa
            COUNT(*) AS _
        FROM
            contracts committ
            -- SOLO i committenti che hanno almeno un dipendente attivo (inner join)
        INNER JOIN employee dipend ON
            dipend.id_committente = committ.id
        WHERE 
            dipend.inizio_contratto <= '{$fine_mese}' 
            AND dipend.fine_contratto >= '{$inizio_mese}'                            
        GROUP BY
            id_committente,
            committente
        HAVING
            COUNT(*) > 0
    ) AS committ_attivi
-- fatture di TUTTI i committenti attivi (left join) (SISTEMA FATTURA 2)
LEFT JOIN fatture2_view fatt2 ON
    fatt2.id_committente = committ_attivi.id
    AND fatt2.anno = {$anno}
    AND fatt2.mese = {$mese}
-- TUTTI i committenti attivi, anche quelli che non hanno fatture (left join)
LEFT JOIN conferme_mensili_per_mese_view conf ON
    conf.id_committente = committ_attivi.id
    AND conf.anno = {$anno}
    AND conf.mese = {$mese}
ORDER BY
    committente ASC
