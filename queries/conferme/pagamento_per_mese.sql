SELECT 
    (EXISTS (
        SELECT *
        FROM 
            conferme
        WHERE
            id_committente = :id_committente
            AND anno = :anno
            AND mese = :mese
            AND conferma = 'PAGAMENTO'
            -- e' necessario esplicitare la logica del -1 
            -- perche' e' stato definito che 
            -- il progressivo nel mese = -1 significa
            -- "sono interessato al mese e non al progressivo nel mese" 
            AND progressivo_nel_mese = -1
    )) AS confermato
