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


/*
COMMITTENTI CHE NON HANNO PAGATO (fattusa=si, pagamento=no)

*/

WITH non_ha_pagato_SUBQ AS (
    SELECT 
        -- campi del committente
        contracts.id AS id_committente,
        contracts.ragione_sociale_appaltante AS committente,
        contracts.commerciale AS commerciale,
        contracts.legale_rappresentante_appaltante AS responsabile,
        contracts.telefono_referente AS telefono,
        contracts.email_referente AS email,
        -- campi delle conferme
        conf.anno AS anno,
        conf.mese AS mese,
        -- campi del recupero. siccome ogni committente qui non e' mai stato
        -- in recupero, allora e' nullo
        NULL AS esito,
        NULL AS note,
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
        conferme_mensili_per_mese_view conf
    -- i committenti selezionati devono avere almeno una conferma (left join)
    LEFT JOIN
        fatture2_view fatt
        ON conf.id_committente = fatt.id_committente
        AND conf.anno = fatt.anno
        AND conf.mese = fatt.mese 
    -- trova i dati dei committenti
    JOIN
        contracts
        ON contracts.id = conf.id_committente
    WHERE
        -- questo significa "non pagato"
        conf.fattura = 1 AND conf.pagamento = 0
        -- seleziona le conferme solo nel periodo dato
        AND conf.anno BETWEEN :start_year AND :end_year
        AND conf.mese BETWEEN :start_month AND :end_month
),
 

/*
COMMITTENTI IN RECUPERO
*/

in_recupero_SUBQ AS (
    SELECT 
        -- campi del committente
        contracts.id AS id_committente,
        contracts.ragione_sociale_appaltante AS committente,
        contracts.commerciale AS commerciale,
        contracts.legale_rappresentante_appaltante AS responsabile,
        contracts.telefono_referente AS telefono,
        contracts.email_referente AS email,
        -- campi delle recupero
        rec.anno AS anno,
        rec.mese AS mese,
        rec.esito AS esito,
        rec.note AS note,
        -- campi delle fatture/totali
        CAST((COALESCE(fatt.totale_gestione,0) 
            + COALESCE(fatt.totale_iva,0) 
            + COALESCE(fatt.totale_stipendio,0)) AS FLOAT) AS totale_fattura,
        -- stato 
        (CASE 
            WHEN rec.esito IN ('pagato', 'chiudere appalto') THEN 'recupero pagato'
            ELSE 'recupero non pagato'
        END) AS stato_credito
    -- inner join: recupero crediti e committenti: seleziona solo i committenti 
    -- che esistono solo nel recupero 
    FROM
        recupero_crediti rec
    -- prendi tutti quelli del recupero (left join)
    LEFT JOIN
        fatture2_view fatt
        ON rec.id_committente = fatt.id_committente
        AND rec.anno = fatt.anno
        AND rec.mese = fatt.mese
    JOIN
        contracts
        ON contracts.id = rec.id_committente 
    WHERE
        -- seleziona i committenti nel recupero solo nel periodo dato
        rec.anno BETWEEN :start_year AND :end_year
        AND rec.mese BETWEEN :start_month AND :end_month
),

/*
COMMITTENTI IN RECUPERO E LAVORATI
*/

in_recupero_lavorati_SUBQ AS (
    SELECT * 
    FROM in_recupero_SUBQ
    WHERE esito IN ('pagato', 'chiudere appalto', 'non vuole pagare')
),

/*
COMMITTENTI IN RECUPERO MA NON LAVORATI 
*/


in_recupero_non_lavorati_SUBQ AS (
    SELECT * 
    FROM in_recupero_SUBQ
    WHERE id_committente NOT IN (SELECT id_committente FROM in_recupero_lavorati_SUBQ)
),


non_ha_pagato_e_in_recupero_mese_uguale_SUBQ AS (
    SELECT 
        non_ha_pagato_SUBQ.*
    FROM 
        non_ha_pagato_SUBQ 
    JOIN
        in_recupero_SUBQ
        ON non_ha_pagato_SUBQ.id_committente = in_recupero_SUBQ.id_committente
        AND non_ha_pagato_SUBQ.anno = in_recupero_SUBQ.anno
        AND non_ha_pagato_SUBQ.mese = in_recupero_SUBQ.mese
),


/*
COMMITTENTI CHE NON HANNO PAGATO MA CHE HANNO COMBINAZIONE DIVERSA DI COMMITTENTE-ANNO-MESE
*/

-- questi committente-anno-mese che verranno inclusi nel risultato,
-- non hanno la conferma al pagamento, ed e' una combinazione diversa 
-- da quello gia' in recupero (quindi potrebbero essere due stessi committenti con lo stesso anno,
-- ma se hanno mese diverse, questa combinazione diversa di committente-anno-mese uscira' nel risultato) 

non_ha_pagato_e_in_recupero_mese_diverso_SUBQ AS (
    SELECT *
    FROM non_ha_pagato_SUBQ 
    
    EXCEPT 
    
    SELECT *
    FROM non_ha_pagato_e_in_recupero_mese_uguale_SUBQ
),


/*
COMMITTENTI DA RECUPERARE 
*/

da_recuperare_SUBQ AS (

    SELECT * 
    FROM in_recupero_non_lavorati_SUBQ
    
    UNION 

    SELECT *
    FROM non_ha_pagato_e_in_recupero_mese_diverso_SUBQ    

)

SELECT *
FROM da_recuperare_SUBQ
ORDER BY 
    totale_fattura DESC, 
    id_committente ASC



