WITH fatturazione_SUBQ AS (
    SELECT
        numero_in_cartella
    FROM 
        -- tabella della societa' con cui e' stato fatturato il committente
        {tabella_documenti_fatturazione}
        --  documenti_soc2
    WHERE 
        id_committente = :id_committente
        AND anno = :anno
        AND mese = :mese
        -- si assume che il tipo di documento fatturazione
        -- sia sempre la specifica
        AND tipo_documento = 'SPECIFICA'
),

confermato_pagamento_SUBQ AS (
    SELECT 
        fatt.numero_in_cartella AS progressivo_nel_mese,
        (CASE WHEN conf.conferma = 'PAGAMENTO' THEN 1 ELSE 0 END) AS confermato
    FROM 
        fatturazione_SUBQ fatt
    LEFT JOIN
        conferme conf
        
        ON conf.id_committente = :id_committente
        AND conf.anno = :anno
        AND conf.mese = :mese

        AND conf.progressivo_nel_mese = fatt.numero_in_cartella
        -- questo e' da darsi per scontato:
        -- un progressivo nel mese che e' gia' stato
        -- inserito in fatturazione, sara' sempre >= 1
        -- esplicitare la logica del >= 1 serve solo
        -- a renderlo esplicito e chiaro
        AND fatt.numero_in_cartella >= 1
)


SELECT
    progressivo_nel_mese,
    confermato
FROM 
    confermato_pagamento_SUBQ
ORDER BY 
    progressivo_nel_mese ASC
    
    
    
    
