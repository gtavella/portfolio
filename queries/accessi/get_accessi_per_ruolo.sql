
WITH lista_pagine AS (
    SELECT 
        pagine.nome AS nome_pagina,
        pagine.url_path AS url_path_pagina,
        pagine.nome_menu AS nome_menu,
        'pagina' AS tipo_elemento,
        -- vero -> vero
        -- falso -> falso
        -- nullo -> falso
        -- solo i casi che mi interessano. gli altri casi assumo che
        -- non ci sia accesso
        COALESCE(CASE
            -- se la visibilita' della pagina non e' stata valorizzata,
            -- allora la visibilita' finale e' ereditata dal menu 
            -- al quale appartiene la pagina
            WHEN ruoli_pagine.puo_accedere IS NULL THEN ruoli_menu.puo_accedere
            -- la visibilita' della pagina prevale sul resto
            WHEN ruoli_pagine.puo_accedere IS TRUE THEN TRUE
            WHEN ruoli_pagine.puo_accedere IS FALSE THEN FALSE
            -- il resto non mi interessa, significa che la visibilita'
            -- e' falso, perche' nullo significa falso
            ELSE NULL
        END, FALSE) AS puo_accedere
    FROM 
        pagine
    LEFT JOIN
        ruoli_pagine
        ON ruoli_pagine.nome_pagina = pagine.nome
        AND ruoli_pagine.nome_ruolo = :ruolo
    LEFT JOIN
        ruoli_menu
        ON ruoli_menu.nome_menu = pagine.nome_menu
        AND ruoli_menu.nome_ruolo = :ruolo
),

lista_menu AS (
    SELECT 
        NULL AS nome_pagina,
        NULL AS url_path_pagina,
        menu.nome AS nome_menu,
        'menu' AS tipo_elemento,
        COALESCE(ruoli_menu.puo_accedere, FALSE) AS puo_accedere
    FROM 
        menu
    LEFT JOIN
        ruoli_menu
        ON ruoli_menu.nome_menu = menu.nome 
        AND ruoli_menu.nome_ruolo = :ruolo
),

n_pagine_per_menu AS (
    SELECT 
        menu.nome AS nome_menu,
        -- se il nome della pagina e' nullo,
        -- cioe' se non esiste almeno una pagina corrispondente
        -- per il menu, allora viene contata come zero 
        -- questi due sono equivalenti (count e sum) ma preferisco
        -- sum perche' e' esplicita
        SUM( IF(nome_pagina IS NULL, 0, 1) ) AS n_pagine
        -- COUNT(nome_pagina) AS n_pagine
    -- includi tutti i menu (left join)
    FROM 
        menu
    LEFT JOIN
        lista_pagine
        ON lista_pagine.nome_menu = menu.nome
     GROUP BY 
        menu.nome
)


SELECT *
FROM (
    SELECT 
        lista_menu.*,
        CAST(pag.n_pagine AS INTEGER) AS n_pagine
    FROM 
        lista_menu
    JOIN 
        n_pagine_per_menu pag
        ON pag.nome_menu = lista_menu.nome_menu
        
    UNION 
    
    SELECT 
        lista_pagine.*,
        NULL AS n_pagine
    FROM 
        lista_pagine
) AS _
ORDER BY 
    nome_menu ASC,
    nome_pagina ASC
