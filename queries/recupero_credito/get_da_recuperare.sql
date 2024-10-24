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
WITH committ_conferma_non_pagato AS (
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
    JOIN
        contracts committ
        ON committ.id = conf.id_committente
    WHERE
        -- filtra per "non pagato"
        conf.fattura = 1
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
        contracts committ
        ON rec.id_committente = committ.id
    WHERE
        -- seleziona le conferme solo nel periodo dato
        rec.anno BETWEEN :start_year AND :end_year
        AND rec.mese BETWEEN :start_month AND :end_month
),

-- i committenti che hanno pagato sono solo quelli che sono in recupero e hanno pagato

committ_recupero_pagato AS (
    SELECT *
    FROM committ_recupero
    WHERE esito = 'pagato'
    ORDER BY totale_fattura DESC, id_committente ASC
),

committ_recupero_chiudere_appalto AS (
    SELECT *
    FROM committ_recupero
    WHERE esito = 'chiudere appalto'
    ORDER BY totale_fattura DESC, id_committente ASC
),

-- in poche parole qualsiasi esito che non e' o pagato o chiudere appalto, si dice non pagato

committ_recupero_non_pagato AS (
    SELECT *
    FROM committ_recupero
    WHERE esito NOT IN ('pagato', 'chiudere appalto')
),

-- i committenti che non hanno pagato sono quelli che non hanno MAI pagato,
-- e quelli che sono in recupero e ancora non hanno pagato
-- ordinali per totale fattura piu alto

-- se il committente-anno-mese in recupero non pagato, esiste anche 
-- in conferma non pagato, includi solo il committente-anno-mese 
-- del recupero

-- seleziona tutti quelli che sono in recupero_non_pagato,
-- e tutti quelli che sono in conferma_non_pagato ma non in recupero_non_pagato



committ_non_pagato_tutti AS (

    SELECT * 
    FROM committ_recupero_non_pagato
    
    UNION 

    -- seleziona i committenti che non hanno la conferma pagamento,
    -- solo se non esistono nel recupero
    SELECT *
    FROM committ_conferma_non_pagato
    WHERE id_committente NOT IN ( SELECT id_committente FROM committ_recupero_non_pagato )

    ORDER BY totale_fattura DESC, id_committente ASC
)

-- questa e' il risultato finale
-- prima ci sono i committenti che non hanno pagato (mai pagato, o in recupero e non pagato)
-- e alla fine i committenti che hanno pagato

-- se esistono committente-anno-mese che non hanno pagato, ma sono gia' in recupero, 
-- mostra solo quello del recupero 

-- in questo modo mi assicuro che il committente esce una sola volta
-- (altrimenti uscirebbe sia perche' non ha pagato in conferme, 
-- sia perche' non ha pagato in recupero)

SELECT *
FROM committ_non_pagato_tutti

-- UNION 

-- SELECT * 
-- FROM committ_recupero_pagato

-- UNION 

-- SELECT *
-- FROM committ_recupero_chiudere_appalto
