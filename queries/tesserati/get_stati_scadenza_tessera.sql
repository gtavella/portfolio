SELECT 
    SUM(CASE 
        WHEN tess.data_scadenza_tessera > CURRENT_DATE + INTERVAL '3 month' THEN 0 
        WHEN tess.data_scadenza_tessera <= CURRENT_DATE + INTERVAL '1 day' THEN 0
        ELSE 1
    END) AS tot_in_scadenza,
    SUM(CASE 
        WHEN tess.data_scadenza_tessera > CURRENT_DATE + INTERVAL '3 month' THEN 0 
        WHEN tess.data_scadenza_tessera <= CURRENT_DATE + INTERVAL '1 day' THEN 1
        ELSE 0
    END) AS tot_scaduti
FROM 
    tesserati tess
WHERE 
    id_ass = {}
