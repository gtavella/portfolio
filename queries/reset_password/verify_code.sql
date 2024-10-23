SELECT *
    FROM
        account
    WHERE 
        email = {email}
        AND email NOT IN (
            SELECT 
                email 
            FROM (
                SELECT 
                    email, 
                    COUNT(*) AS tot
                FROM
                    codici_reset_passw
                WHERE 
                    email = {email}
                    AND usato = false
                    AND created_at >= NOW() - INTERVAL '1 hour'
                GROUP BY 
                    email
                HAVING
                    COUNT(*) >= 1
            ) AS _
        ) 
