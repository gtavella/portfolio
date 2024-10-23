SELECT
        id_committente,
        committente
FROM (
        SELECT
            committ.id AS id_committente,
            committ.ragione_sociale_appaltante AS committente,
            -- quanti dipendenti attivi per committente,
            -- non mi interessa   
            COUNT(*) AS _     
        FROM
                contracts committ
        -- seleziona solo i committenti il cui numero di dipendenti attivi
        -- e' >= 1 (inner join)
        INNER JOIN employee dipend  
                ON committ.id = dipend.id_committente
        -- considera anche il committente che non ha mai avuto nessun tipo di conferma
        -- perche' questo implica che neanche la sua conferma fattura esiste,
        -- e quindi va incluso nel risultato (left join)
        LEFT JOIN conferme_mensili_per_mese_view conf 
                ON committ.id = conf.id_committente 
                AND conf.anno = ?
                AND conf.mese = ?
        WHERE
            dipend.inizio_contratto <= ?
            AND dipend.fine_contratto >= ?
            -- committenti non pagati: ha fattura E non ha pagato
            AND conf.fattura = 1
            AND (conf.pagamento = 0 
                 OR conf.pagamento IS NULL)
            {$sql_commerciale}
        GROUP BY 
            committ.id
) AS _
ORDER BY 
    committente ASC
