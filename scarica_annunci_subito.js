/*
Ottieni un csv che si scarica immediatamente, dagli annunci di Subito.it

pagina esempio:
https://impresapiu.subito.it/shops/48276-lavora-con-noi-italia-srls?page=1

PASSI:
1. copia questo codice nella console del browser
2. esegui questo codice, nello specifico questa funzione:

scarica_annunci_di_questa_pagina() 

*/

function scarica_annunci_di_questa_pagina() {
    const lista_annunci = ottieni_annunci_subito_di_questa_pagina()
    const csv_str = crea_str_csv_da_lista(lista_annunci)
    crea_e_scarica_csv_da_str({
        csv_str
    })
}


function crea_e_scarica_csv_da_str({csv_str, filename = "risultati.csv"}) 
{
    const risultato = crea_csv_da_str(csv_str)
    const {blob, blob_url} = risultato
    scarica_file({
        url: blob_url,
        filename
    })
}


function crea_csv_da_str(csvString) {
    const blob = new Blob([csvString], { type: 'text/csv' })
    return {
        blob,
        blob_url: URL.createObjectURL(blob)
    }
}


function scarica_file({url, filename}) 
{
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    link.click()
}



function crea_str_csv_da_lista(lista_ogg) {
    if (!lista_ogg.length) return "";

    const colonne_lista = Object.keys(lista_ogg[0]);
    const colonne_str = colonne_lista.join(",");

    const righe = lista_ogg.map(ogg =>
        colonne_lista.map(colonna =>
            String(ogg[colonna]).replace(/"/g, '""') // escape doppie virgolette
        ).map(val => `"${val}"`).join(",") // racchiudi ogni valore tra virgolette
    );

    return [colonne_str, ...righe].join("\n");
}




function ottieni_annunci_subito_di_questa_pagina() {

    var ret = []
    var $items_listing = $("ul.items_listing")
    var $items = $items_listing.find("li")
    // modifica quanti annunci prendere dal container
    var $some_items = $items.slice(0, $items.length)
    
    $some_items.each((i, item_el) => {
         let $item = $(item_el)
        
         let $item_container = $item
                                 .find("article")
                                 .find("div.item_list_inner")
        
         let $item_content = $item_container
                                             .find("div.item_description")
        
         let $title = $item_content
                                 .find("h2")
                                 .find("a")
        
         let $info = $item_content
                                .find("span.item_info")
         
         // componenti specifiche all'interno dell'info 
         let $location = $info
                           .find("span.item_location")
        
         let titolo_iniziale = $title
                                 .text()
                                 .trim()
        
         let titolo_iniziale_splittato_arr = titolo_iniziale
                                                 .split('-')
        
         let titolo = titolo_iniziale_splittato_arr
                                     .slice(0, -1)
                                     .join("-")
        
         let id_annuncio = titolo_iniziale_splittato_arr
                                 [titolo_iniziale_splittato_arr.length - 1]
                                 .split("#")
                                 .slice(1)
                                 .join("")
                                 
                                 
             
         let quando = $info
                         .find("time")
                         .text()
                         .trim()
        
          let location = $location
                              .text()
                              .replace(/\s+/g, ' ')
                              .trim()
          ret.push({
              titolo,
              id_annuncio,
              quando,
              dove: location
          }) 
    })

    return ret 
}
