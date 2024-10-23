-- tabelle coinvolte: committenti, fatture, recupero crediti
-- quindi nella tabella finale devono uscire i committenti che:
-- - non hanno pagato
-- - hanno pagato, ma sono stati in recupero credito
-- - sono ancora in recupero credito

/*   

STATI

    ha pagato?  |  e' stato in recupero?    |  mostra?   |        stato credito          |       
-------------------------------------------------------------------------------------
       SI                 SI                      SI            recupero pagato          
       SI                 NO                      NO            [non rilevante]
       NO                 SI                      SI            recupero non pagato
       NO                 NO                      SI            mai pagato 
*/

-- Se io metto conferma pagato dal sistema fatturazione, 
-- non dovrà piu comparire nel sistema recupero crediti. 
-- Se io invece seleziono pagato nell'esito del recupero crediti, 
-- mi mette il flag in automatico su pagato (nel sistema fatturazione), ma dal sistema recupero non sparisce



-- seleziona i committenti che non hanno pagato, solo se non hanno pagato
-- fattura=si E pagato=no
-- i loro totali (gestione, stipendio ecc.) possono essere o anche no.
-- 
WITH committ_mai_pagato AS (
    SELECT 
        -- campi del committente
        committ.id AS id_committente,
        committ.ragione_sociale_appaltante AS committente,
        committ.commerciale AS commerciale,
        committ.legale_rappresentante_appaltante AS responsabile,
        committ.telefono_referente AS telefono,
        committ.email_referente AS email,
        -- campi delle conferme
        conf.anno AS anno,
        conf.mese AS mese,
        -- campi delle fatture/totali
        CAST((COALESCE(fatt.totale_gestione,0) 
        + COALESCE(fatt.totale_iva,0) 
        + COALESCE(fatt.totale_stipendio,0)) AS FLOAT) AS totale_fattura,
        -- dati sullo stato
        'mai pagato' AS stato_credito
    -- inner join tra committenti e conferme: seleziona i committenti che "non hanno pagato",
    -- solo se non hanno pagato. ignora gli altri committenti, e ignora i committenti che 
    -- non hanno mai avuto una conferma
    FROM 
         contracts committ
    JOIN
        conferme_mensili_per_mese_view conf
        ON committ.id = conf.id_committente
    -- ? join tra committenti che non hanno pagato, e trova i loro totali/totale fattura
    -- un committente potrebbe anche non avere mai avuto un totale?
    RIGHT JOIN
        fatture2_view fatt
        ON committ.id = fatt.id_committente
    WHERE
        -- seleziona solo le fatture il cui periodo 
        -- e' uguale a quelle delle conferme  
        conf.anno = fatt.anno
        AND conf.mese = fatt.mese
        -- filtra per "non pagato"
        AND conf.fattura = 1
        AND conf.pagamento = 0
        -- seleziona le conferme solo nel periodo dato
        AND conf.anno BETWEEN :start_year AND :end_year
        AND conf.mese BETWEEN :start_month AND :end_month
),
 
committ_recupero AS (
    SELECT 
        -- campi del committente
        committ.id AS id_committente,
        committ.ragione_sociale_appaltante AS committente,
        committ.commerciale AS commerciale,
        committ.legale_rappresentante_appaltante AS responsabile,
        committ.telefono_referente AS telefono,
        committ.email_referente AS email,
        -- campi delle recupero
        rec.anno AS anno,
        rec.mese AS mese,
        -- campi delle fatture/totali
        CAST((COALESCE(fatt.totale_gestione,0) 
        + COALESCE(fatt.totale_iva,0) 
        + COALESCE(fatt.totale_stipendio,0)) AS FLOAT) AS totale_fattura,
        -- stato 
        (CASE 
            WHEN rec.esito = 'pagato' THEN 'recupero pagato'
            ELSE 'recupero non pagato'
        END) AS stato_credito
    -- inner join: recupero crediti e committenti: seleziona solo i committenti 
    -- che esistono solo nel recupero 
    FROM
        recupero_crediti rec
    JOIN
        contracts committ
        ON rec.id_committente = committ.id
    RIGHT JOIN
        fatture2_view fatt
        ON committ.id = fatt.id_committente
    WHERE
        -- anno e mese del recupero devono essere uguali ad anno e mese dei totali 
        rec.anno = fatt.anno
        AND rec.mese = fatt.mese
        -- seleziona le conferme solo nel periodo dato
        AND rec.anno BETWEEN :start_year AND :end_year
        AND rec.mese BETWEEN :start_month AND :end_month
)




SELECT *
FROM 
    committ_mai_pagato

UNION 

SELECT *
FROM 
    committ_recupero

ORDER BY 
    -- i totali fattura piu' grandi sono i primi
    totale_fattura DESC
