-- determina il mese di fine, in cui vi e' la massima (o piu' lontana nel futuro)
-- data di presa appt, o data fissata per l'appt
-- cosi sei sicuro che in qualunque caso, non solo si sta considerando
-- il mese di fine piu lontano nel futuro, ma anche il mese di fine 
-- piu lontano nel futuro del mese tra il massimo del mese tra 
-- la data appt e la data presa appt

-- MESE & ANNO DI FINE

WITH RECURSIVE Q00_1 AS (
   SELECT 
        -- anno inizio
        CAST((SELECT anno_inizio FROM Q1) AS UNSIGNED) 
            AS anno,
        -- mese inizio
        CAST((SELECT mese_inizio FROM Q1) AS UNSIGNED) 
            AS mese,
        DATE(CONCAT_WS('-', 
                      (SELECT anno_inizio FROM Q1), 
                      LPAD( (SELECT mese_inizio FROM Q1), 2, '0'), '01')) 
                      AS prima_data_mese
    WHERE 
        (CAST((SELECT anno_fine FROM Q0) AS UNSIGNED) > CAST((SELECT anno_inizio FROM Q1) AS UNSIGNED)) 
        OR (
            CAST((SELECT anno_fine FROM Q0) AS UNSIGNED) = CAST((SELECT anno_inizio FROM Q1) AS UNSIGNED) 
            AND CAST((SELECT mese_fine FROM Q0) AS UNSIGNED) >= CAST((SELECT mese_inizio FROM Q1) AS UNSIGNED)
        )
    UNION ALL
    SELECT 
        YEAR(DATE_ADD(prima_data_mese, INTERVAL 1 MONTH)),
        MONTH(DATE_ADD(prima_data_mese, INTERVAL 1 MONTH)),
        DATE_ADD(prima_data_mese, INTERVAL 1 MONTH)
    FROM 
        Q00_1
    WHERE 
        prima_data_mese < DATE(CONCAT_WS('-', (SELECT anno_fine FROM Q0), LPAD((SELECT mese_fine FROM Q0), 2, '0'), '01'))
),

-- inserisci la data fine del mese

Q00_2 AS (
    SELECT 
        *,
        LAST_DAY(prima_data_mese)
            AS ultima_data_mese
    FROM 
        Q00_1
),

-- il mese cuscinetto precedente 
-- al mese piu piccolo da cui cominciare le stats

Q00_3 AS (
    SELECT 
        YEAR(DATE_SUB((SELECT prima_data_mese_inizio FROM Q1), INTERVAL 1 MONTH))
            AS anno,
        MONTH(DATE_SUB((SELECT prima_data_mese_inizio FROM Q1), INTERVAL 1 MONTH))
            AS mese,
        DATE_SUB((SELECT prima_data_mese_inizio FROM Q1), INTERVAL 1 MONTH)
            AS prima_data_mese,
        LAST_DAY(DATE_SUB((SELECT prima_data_mese_inizio FROM Q1), INTERVAL 1 MONTH))
            AS ultima_data_mese
),

-- il mese cuscinetto successivo 
-- al mese piu grande dove finire le stats

Q00_4 AS (
    SELECT 
        YEAR(DATE_ADD((SELECT ultima_data_mese_fine FROM Q0), INTERVAL 1 MONTH))
            AS anno,
        MONTH(DATE_ADD((SELECT ultima_data_mese_fine FROM Q0), INTERVAL 1 MONTH))
            AS mese,
        DATE_ADD(CONCAT((SELECT anno_fine FROM Q0), '-', (SELECT mese_fine FROM Q0), '-', '01'), INTERVAL 1 MONTH)
            AS prima_data_mese,
        LAST_DAY(DATE_ADD((SELECT ultima_data_mese_fine FROM Q0), INTERVAL 1 MONTH))
            AS ultima_data_mese
),


-- aggiungi i mesi cuscinetto all inizio e alla fine, per 
-- assicurarsi che se una settimana sta a cavallo tra 2 mesi, 
-- sicuramente anche quel mese in cui la settimana continua,
-- viene anche considerato

Q00_5 AS (

    -- aggiungi il mese cuscinetto precedente 

    SELECT 
        anno,
        mese,
        prima_data_mese,
        ultima_data_mese
    FROM 
        Q00_3

    UNION ALL 
    
    -- aggiungi  i mesi nel range dal mese piu piccolo 
    -- a quello piu lontano nel futuro 

        SELECT 
        anno,
        mese,
        prima_data_mese,
        ultima_data_mese
    FROM 
        Q00_2
    
    UNION ALL 

    -- aggiungi il mese cuscinetto successivo 
    SELECT 
        anno,
        mese,
        prima_data_mese,
        ultima_data_mese
    FROM 
        Q00_4
),

/*
IL RANGE DEI MESI IN CUI CALCOLARE
*/

Q00 AS (
    SELECT * 
    FROM Q00_5
),


/*
IL RANGE DI SETTIMANE:

la prima settimana in cui partire in assoluto, 
e l ultima settimana lun-ven del mese precedente al mese 
piu lontano nel passato in cui esiste almeno un appt

l ultima settimana in cui finire in assoluto,
e la prima settimana lun-ven del mese successivo al mese 
piu lontano nel futuro in cui esiste almeno un appt

*/


Q01_1 AS (
    SELECT 

        -- Ultimo lunedì del mese precedente al mese minimo
        DATE_SUB(LAST_DAY(CONCAT((SELECT anno FROM Q00_3), '-', (SELECT mese FROM Q00_3), '-01')), 
                INTERVAL (WEEKDAY(LAST_DAY(CONCAT((SELECT anno FROM Q00_3), '-', (SELECT mese FROM Q00_3), '-01'))) - 0 + 7) % 7 DAY) 
        AS ultimo_lunedi_di_mese_prec_di_mese_min,
        
        -- Primo lunedì del mese successivo al mese massimo
            DATE_ADD(DATE(CONCAT((SELECT anno FROM Q00_4), '-', (SELECT mese FROM Q00_4), '-01')), 
                INTERVAL (7 - WEEKDAY(DATE(CONCAT((SELECT anno FROM Q00_4), '-', (SELECT mese FROM Q00_4), '-01')))) % 7 DAY) 
        AS primo_lunedi_di_mese_succ_di_mese_max
),

-- crea i range di settimana

Q01_2 AS (
    SELECT 
        DATE_FORMAT((SELECT ultimo_lunedi_di_mese_prec_di_mese_min FROM Q01_1), '%Y-%m-%d') 
            AS data_inizio_settimana,
        DATE_ADD(DATE_FORMAT((SELECT ultimo_lunedi_di_mese_prec_di_mese_min FROM Q01_1), '%Y-%m-%d'), INTERVAL 4 DAY) 
            AS data_fine_settimana

    UNION ALL

    SELECT 
        DATE_ADD(data_inizio_settimana, INTERVAL 7 DAY),
        DATE_ADD(data_fine_settimana, INTERVAL 7 DAY)
    FROM 
        Q01_2
    WHERE 
        data_fine_settimana < (SELECT primo_lunedi_di_mese_succ_di_mese_max FROM Q01_1)
),

-- 

Q01 AS (
    SELECT * 
    FROM Q01_2
),


/*
TUTTI I PERIODI (MESI & SETTIMANE)
CHE VENGONO D ORA IN POI GENERALIZZATI CON  
DATA INIZIO E DATA FINE
*/

Q02_1 AS (
    -- I MESI
    SELECT 
        anno
            AS anno_inizio,
        anno 
            AS anno_fine,
        mese
            AS mese_inizio,
        mese 
            AS mese_fine,
        prima_data_mese
            AS data_inizio,
        ultima_data_mese
            AS data_fine,
        'mese'
            AS livello_dettaglio
    FROM 
        Q00
    
    UNION ALL 
    -- LE SETTIMANE

    SELECT         
        YEAR(data_inizio_settimana)
            AS anno_inizio,
        YEAR(data_fine_settimana)
            AS anno_fine,
        MONTH(data_inizio_settimana)
            AS mese_inizio,
        MONTH(data_fine_settimana)
            AS mese_fine,
        data_inizio_settimana
            AS data_inizio,
        data_fine_settimana
            AS data_fine,
        'settimana lavorativa'
            AS livello_dettaglio
    FROM 
        Q01
),

-- togli i mesi e le settimane, la cui settimana finale o iniziale 
-- non ha stats?  

Q02_2 AS (
    SELECT *
    FROM Q02_1
),

Q02 AS (
    SELECT * 
    FROM Q02_2
),

-- una semplice lista dei mesi

Q03 AS (
        SELECT 
          'gennaio' AS mese_nome, 'gen' AS mese_nome_breve, 1 AS mese_numero
        UNION ALL
        SELECT 'febbraio', 'feb', 2
        UNION ALL
        SELECT 'marzo', 'mar', 3
        UNION ALL
        SELECT 'aprile', 'apr', 4
        UNION ALL
        SELECT 'maggio', 'mag', 5
        UNION ALL
        SELECT 'giugno', 'giu', 6
        UNION ALL
        SELECT 'luglio', 'lug', 7
        UNION ALL
        SELECT 'agosto', 'ago', 8
        UNION ALL
        SELECT 'settembre', 'set', 9
        UNION ALL
        SELECT 'ottobre', 'ott', 10
        UNION ALL
        SELECT 'novembre', 'nov', 11
        UNION ALL
        SELECT 'dicembre', 'dic', 12
),


Q0_1 AS (
    SELECT 
        MAX(data_appt)
            AS data_appt_max,
        MAX(data_presa_appt)
            AS data_presa_appt_max
    FROM 
        appuntamenti
),


Q0_2 AS (
    SELECT 
        GREATEST(data_appt_max, data_presa_appt_max)
            AS data_max
    FROM 
        Q0_1
),

/*
colonne:
    mese_fine
    anno_fine
*/

Q0_3 AS (
    SELECT 
        EXTRACT(MONTH FROM data_max)
            AS mese_fine,
        EXTRACT(YEAR FROM data_max)
            AS anno_fine
    FROM 
        Q0_2
),


Q0 AS (
    SELECT 
        *,
        LAST_DAY(CONCAT(anno_fine, '-', mese_fine, '-', '01'))
            AS ultima_data_mese_fine
    FROM 
        Q0_3
),


Q1_1 AS (
    SELECT 
        MIN(data_appt)
            AS data_appt_min,
        MIN(data_presa_appt)
            AS data_presa_appt_min
    FROM 
        appuntamenti
),


Q1_2 AS (
    SELECT 
        LEAST(data_appt_min, data_presa_appt_min)
            AS data_min
    FROM 
        Q1_1
),

/*
colonne:
    mese_inizio
    anno_inizio
*/

Q1_3 AS (
    SELECT 
        EXTRACT(MONTH FROM data_min)
            AS mese_inizio,
        EXTRACT(YEAR FROM data_min)
            AS anno_inizio
    FROM 
        Q1_2
),




Q1 AS (
    SELECT 
        *,
        CONCAT(anno_inizio, '-', mese_inizio, '-', '01')
            AS prima_data_mese_inizio
    FROM 
        Q1_3
),



-- estrai anno e mese da tutti gli appuntamenti

Q3 AS (
    SELECT 
        id,
        data_appt,
        data_presa_appt,
        esito,
        EXTRACT(YEAR FROM data_appt)
            AS anno_data_appt,
        EXTRACT(MONTH FROM data_appt)
            AS mese_data_appt,
        EXTRACT(YEAR FROM data_presa_appt)
            AS anno_data_presa_appt,
        EXTRACT(MONTH FROM data_presa_appt)
            AS mese_data_presa_appt
    FROM 
        appuntamenti
),


/*
CALCOLA LE STATS NEI RANGE (MESI E SETTIMANE)
*/

-- tot appt presi

Q4_1 AS (
    SELECT 
        A.data_inizio   
                AS data_inizio,
        A.data_fine
                AS data_fine,
        COUNT(B.id) 
            AS tot_appt_presi
    FROM 
        Q02 A
    LEFT JOIN 
        Q3 B
        ON (
            B.data_presa_appt >= A.data_inizio
            AND B.data_presa_appt <= A.data_fine
        )
    GROUP BY 
        A.data_inizio,
        A.data_fine    
),

-- tot appt esitati

Q4_2 AS (
    SELECT 
        A.data_inizio   
                AS data_inizio,
        A.data_fine
                AS data_fine,
        COUNT(B.id) 
            AS tot_appt_esitati
    FROM 
        Q02 A
    LEFT JOIN 
        Q3 B
        ON (
            B.data_appt >= A.data_inizio
            AND B.data_appt <= A.data_fine
            AND B.esito != 'in attesa di esito'
        )
    GROUP BY 
        A.data_inizio,
        A.data_fine  
),

-- tot appt non esitati


Q4_3 AS (
    SELECT 
        A.data_inizio   
                AS data_inizio,
        A.data_fine
                AS data_fine,
        COUNT(B.id) 
            AS tot_appt_non_esitati
    FROM 
        Q02 A
    LEFT JOIN 
        Q3 B
        ON (
            B.data_appt >= A.data_inizio
            AND B.data_appt <= A.data_fine
            AND B.esito = 'in attesa di esito'
        )
    GROUP BY 
        A.data_inizio,
        A.data_fine  
),

-- tot appt firma contratto

Q4_4 AS (
    SELECT 
        A.data_inizio   
                AS data_inizio,
        A.data_fine
                AS data_fine,
        COUNT(B.id) 
            AS tot_appt_firma_precontratto
    FROM 
        Q02 A
    LEFT JOIN 
        Q3 B
        ON (
            B.data_appt >= A.data_inizio
            AND B.data_appt <= A.data_fine
            AND B.esito = 'firma precontratto'
        )
    GROUP BY 
        A.data_inizio,
        A.data_fine  
),


-- tot appt inviato preventivo

Q4_5 AS (
    SELECT 
        A.data_inizio   
                AS data_inizio,
        A.data_fine
                AS data_fine,
        COUNT(B.id) 
            AS tot_appt_preventivo_inviato
    FROM 
        Q02 A
    LEFT JOIN 
        Q3 B
        ON (
            B.data_appt >= A.data_inizio
            AND B.data_appt <= A.data_fine
            AND B.esito = 'inviato preventivo'
        )
    GROUP BY 
        A.data_inizio,
        A.data_fine  
),



-- tot appt inviato preventivo

Q4_6 AS (
    SELECT 
        A.data_inizio   
                AS data_inizio,
        A.data_fine
                AS data_fine,
        COUNT(B.id) 
            AS tot_appt_interessato
    FROM 
        Q02 A
    LEFT JOIN 
        Q3 B
        ON (
            B.data_appt >= A.data_inizio
            AND B.data_appt <= A.data_fine
            AND B.esito = 'interessato'
        )
    GROUP BY 
        A.data_inizio,
        A.data_fine  
),



-- tot appt rinviato o non presente

Q4_7 AS (
    SELECT 
        A.data_inizio   
                AS data_inizio,
        A.data_fine
                AS data_fine,
        COUNT(B.id) 
            AS tot_appt_rinviato_o_non_presente
    FROM 
        Q02 A
    LEFT JOIN 
        Q3 B
        ON (
            B.data_appt >= A.data_inizio
            AND B.data_appt <= A.data_fine
            AND B.esito IN ('rinviato', 'non presente')
        )
    GROUP BY 
        A.data_inizio,
        A.data_fine  
),


-- unisci tutte le colonne delle stats (tot appt esitati, presi ecc.)
-- con quelle relativi ai dati sul periodo (data inizio, livello dettaglio, anno, mese ecc.)

Q4 AS (
    SELECT 
        A.*,
        B.tot_appt_presi,
        C.tot_appt_esitati,
        D.tot_appt_non_esitati,
        E.tot_appt_firma_precontratto,
        F.tot_appt_preventivo_inviato,
        G.tot_appt_interessato,
        H.tot_appt_rinviato_o_non_presente
    FROM 
        Q02 A
    JOIN 
        Q4_1 B
        ON (
            A.data_inizio = B.data_inizio
            AND A.data_fine = B.data_fine
        )
    JOIN 
        Q4_2 C
        ON (
            A.data_inizio = C.data_inizio
            AND A.data_fine = C.data_fine
        )
    JOIN 
        Q4_3 D
        ON (
            A.data_inizio = D.data_inizio
            AND A.data_fine = D.data_fine
        )
    JOIN 
        Q4_4 E
        ON (
            A.data_inizio = E.data_inizio
            AND A.data_fine = E.data_fine
        )
    JOIN 
        Q4_5 F
        ON (
            A.data_inizio = F.data_inizio
            AND A.data_fine = F.data_fine
        )
    JOIN 
        Q4_6 G
        ON (
            A.data_inizio = G.data_inizio
            AND A.data_fine = G.data_fine
        )
    JOIN 
        Q4_7 H
        ON (
            A.data_inizio = H.data_inizio
            AND A.data_fine = H.data_fine
        )
),


-- completa con dati di comodita (nome del mese ecc.)

Q5 AS (
    SELECT 
        *,

    (CASE 
        WHEN livello_dettaglio = 'mese'
         THEN  (
            SELECT A.mese_nome FROM Q03 A WHERE A.mese_numero = mese_inizio
         ) 
         ELSE null
    END) 
        AS nome_mese,

        (CASE 
           WHEN livello_dettaglio = 'settimana lavorativa' 
              THEN (
                mese_inizio != mese_fine
              )
           ELSE null
        END) 
            AS settimana_lavorativa_e_tra_mesi
    FROM 
        Q4
),


Q6_1 AS (
        -- aggiungi l'etichetta di una settimana lavorativa,
        -- che dipende se la settimana sta a cavallo (è tra)
        -- due mesi
        SELECT 
                A.*,
                (CASE
                     WHEN A.livello_dettaglio = 'mese' THEN null
                     WHEN A.settimana_lavorativa_e_tra_mesi = true
                         THEN 
                             CONCAT(
                                DAY(A.data_inizio),
                                '-',
                                DAY(A.data_fine),
                                ' ',
                                B.mese_nome_breve,
                                '/',
                                C.mese_nome_breve
                             )
                         ELSE 
                             CONCAT (
                                DAY(A.data_inizio),
                                '-',
                                DAY(A.data_fine),
                                ' ',
                                B.mese_nome_breve                               
                             )
                 END)
                        AS etichetta_settimana_lavorativa
         FROM 
                Q5 A
         LEFT JOIN
                Q03 B
                ON B.mese_numero = A.mese_inizio
         LEFT JOIN
                Q03 C
                ON C.mese_numero = A.mese_fine
                
),

-- ci sara solo un'etichetta finale che viene
-- mostrata all'utente, indipendentemente 
-- che si tratti di un mese o settimana

Q6_2 AS (
        SELECT 
                *,
                (CASE 
                        WHEN livello_dettaglio = 'mese'
                                THEN nome_mese
                        WHEN livello_dettaglio = 'settimana lavorativa'
                                THEN etichetta_settimana_lavorativa
                        ELSE 
                                '(non valida)'
                END) 
                        AS etichetta
        FROM 
                Q6_1
)



SELECT * 
FROM Q6_2
ORDER BY 
    data_inizio ASC,
    data_fine ASC
