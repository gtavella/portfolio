/*

l'output deve contenere:
tot_committenti_recuperati
tot_committenti_da_recuperare
tot_committenti_chiudere_appalto
tot_committenti_non_vuole_pagare
tot_committenti_lavorati

tot_soldi_recuperati
tot_soldi_da_recuperare

flusso: 
ottieni id POI conta quanti sono POI calcola i loro totali


committenti:
recuperati
da recuperare
lavorati

*/

-- i committenti e i loro anni-mesi e stati
WITH recupero_stati AS (
    SELECT 
        rec.id_committente AS id_committente,
        rec.anno AS anno,
        rec.mese AS mese,
        (CASE WHEN rec.esito = 'pagato' THEN true ELSE false END) AS recuperato,
        (CASE WHEN rec.esito IN ('PDR accettato', 'da richiamare') THEN true ELSE false END) AS da_recuperare,
        (CASE WHEN rec.esito IN ('non pagato', 'chiudere appalto') THEN true ELSE false END) AS perso,
        (CASE WHEN CHAR_LENGTH(note) > 0 THEN true ELSE false END) AS lavorato,
        (CASE WHEN rec.esito = 'chiudere appalto' THEN true ELSE false END) AS chiudere_appalto,
        (CASE WHEN rec.esito = 'non vuole pagare' THEN true ELSE false END) AS non_vuole_pagare
    FROM 
        recupero_crediti rec
),


-- totali di tutte le proprieta', per ogni mese.
-- invece di scrivere una sottoquery/CTE per ogni proprieta,
-- utilizzo "aggiungere lo 0" come sinonimo di "quella proprieta' ha valore falso" 

recupero_totali_committenti_per_mese AS (
    SELECT 
        anno,
        mese,
        SUM(CASE WHEN recuperato = true THEN 1 ELSE 0 END) AS recuperati,
        SUM(CASE WHEN da_recuperare = true THEN 1 ELSE 0 END) AS da_recuperare,
        SUM(CASE WHEN chiudere_appalto = true THEN 1 ELSE 0 END) AS chiudere_appalto,
        SUM(CASE WHEN non_vuole_pagare = true THEN 1 ELSE 0 END) AS non_vuole_pagare,
        SUM(CASE WHEN lavorato = true THEN 1 ELSE 0 END) AS lavorati
    FROM 
        recupero_stati
    GROUP BY
        anno,
        mese
),

recupero_totali_soldi_per_mese AS (
    SELECT 
        rec.anno AS anno,
        rec.mese AS mese,
        
        -- recuperati
        SUM(CASE WHEN rec.recuperato = true 
                        THEN fatt.totale_gestione 
                             + fatt.totale_iva 
                             + fatt.totale_stipendio 
                 ELSE 0 END) AS recuperati,

        -- da recuperare
        SUM(CASE WHEN rec.da_recuperare = true 
                        THEN fatt.totale_gestione 
                             + fatt.totale_iva 
                             + fatt.totale_stipendio 
                 ELSE 0 END) AS da_recuperare

    FROM 
        recupero_stati rec
    JOIN
        fatture2_view fatt
        ON fatt.id_committente = rec.id_committente
        AND fatt.anno = rec.anno
        AND fatt.mese = rec.mese
    GROUP BY
        rec.anno,
        rec.mese
)


-- SELECT *
-- FROM totali_soldi_per_mese


-- 2) Tot. recuperato è la somma delle fatture di quelli stati in recuperato che hanno come esito PAGATO
-- 3) Tot. committenti recuperato è la somma dei committenti che  stati in recupero hanno come esito PAGATO
-- 4) Tot. da recuperare DIFFERENZA TRA Totale nella tabella recupero crediti - totale recuperato??
-- 5) committenti da recuperare sono tutti quelli che hanno esito da richiamare o pdr accettato
-- 6) tot committenti lavorati, sono tutti quelli in recupero che hanno avuto un esito qualsiasi


SELECT 
    rec.anno AS anno,
    rec.mese AS mese,
    -- committenti
    CAST(rec_tot_comm.recuperati AS INT) AS tot_committenti_recuperati,
    CAST(rec_tot_comm.da_recuperare AS INT) AS tot_committenti_da_recuperare,
    CAST(rec_tot_comm.chiudere_appalto AS INT) AS tot_committenti_chiudere_appalto,
    CAST(rec_tot_comm.non_vuole_pagare AS INT) AS tot_committenti_non_vuole_pagare,
    CAST(rec_tot_comm.lavorati AS INT) AS tot_committenti_lavorati,
    -- soldi
    CAST(rec_tot_soldi.recuperati AS FLOAT) AS tot_soldi_recuperati,
    CAST(rec_tot_soldi.da_recuperare AS FLOAT) AS tot_soldi_da_recuperare
FROM 
    recupero_stati rec
JOIN
    recupero_totali_committenti_per_mese rec_tot_comm
    ON rec.anno = rec_tot_comm.anno
    AND rec.mese = rec_tot_comm.mese
JOIN
    recupero_totali_soldi_per_mese rec_tot_soldi
    ON rec.anno = rec_tot_soldi.anno
    AND rec.mese = rec_tot_soldi.mese
GROUP BY
    rec.anno,
    rec.mese
ORDER BY 
    rec.anno DESC, 
    rec.mese DESC
