
var _g672 = {
    "counter": 1
};


$(document).ready(function() {

    var load_more_enabled = $("#load-more").length > 0 && $("#sort_method").length > 0;
    
    // only implement logic for loading more and sorting
    // if the html elements that allow it are present 
    if (load_more_enabled) {
        // if last pagination info is None, it's interpreted as
        // the server doesn't already have items, so
        // allowing the user to load more doesn't make sense
        if (gctx["pagin_last"] == "None") {
            // $load_more_btn.attr("disabled", true);
        }
    
    }
    else {
        console.log("'Load more' functionality not enabled: reason: " +
                    "the load more button and/or " +
                    "the sort method options have not been detected.")
    }


});



function load_more_items(self, sfid, endpoint) {
    // self is the button itself that 
    // when clicked, triggers the load more items now logic
    
    var $load_more_btn = $(self);

    var $spinner = $spinner_template.clone();
    // get the inside html content of the 
    // button that triggered the more data loading
    var $content_load_more_btn = $load_more_btn.children().detach();
    
    // disable button
    $load_more_btn.attr("disabled", true);
    // replace load more button content with spinner
    $load_more_btn.html($spinner);


    // Send AJAX request
    $.ajax({
        url: build_url(sfid, endpoint),
        method: 'GET',
        success: function(resp) {
            
            // re-enable load more button because 
            // network request is in progress
            $load_more_btn.removeAttr("disabled");
            $load_more_btn.html($content_load_more_btn);
            
            // if there are no more items
            if (!resp.are_more) {
                // this timeout is needed to 
                // allow the dom to perform the previous actions
                // before the alert is shown
                setTimeout(() => {
                    alert("Non ci sono più dati per adesso");
                }, 50); 
                return;
            } 

            // update the new pagination as the last pagination 
            // the server will instruct you  
            // the server will set last pagination info to null/None 
            // when there are no more elements
            if (resp.pagin_last != null) {
                gctx["pagin_last"] = resp.pagin_last;
            } 

        
            // Append rows in chunks
            appendHtmlRows(resp.html); 

        },
        error: function(xhr, status, error) {
            $load_more_btn.removeAttr("disabled");
            $load_more_btn.html($content_load_more_btn);
            alert("Errore nei server");
            console.error('Error loading data:', error);
        }
    });
}



function appendHtmlRows(html_rows) {
    // html rows is just a string

    // it seems like when parsing html from the
    // html string, a weird text html element 
    // appears, so i make sure that only 
    // table rows are inserted
    var $rows = $(html_rows).filter("tr");

    changeColorRows($rows, _g672);

    // Redraw the table to ensure it processes the new rows
    gctx["table"].rows.add($rows).draw(false); // `false` keeps the current page
}



function build_url(sfid, endpoint) {
    /*
    "entity" is the type of data (tesserati, entrate, 
    cariche sociali) etc. that i want
    */
    
    var sort_method = get_sort_method_from_qstr();

    if (sort_method == null) {
        sort_method = "desc";
    }

    // get query string from this object
    let qstr = obj_to_qstr({
        "pagin_last": gctx["pagin_last"],
        "sort_method": sort_method
    });  

    // transform the interested form into a query string
    // for this to work, follow the standard: 
    // the form must have as ID with this format: search_filter_<entity>,
    // for example: search_filter_entrate, 
    // search_filter_tesserati, search_filter_soci etc.
    if (gctx["sfon"]) {
        qstr += "&" + form_to_qstr(sfid)
    }

    // final url to send GET request to, 
    // with query string complete 
    return `${endpoint}?${qstr}&out_format=html`;

}



function changeColorRows($rows, g) {
    if (g["counter"] % 2 != 0) {
        $rows.css('background-color', '#F0F0F0');
    }
    g["counter"] += 1;
}



function update_sort_method(self) {
    // self is the html select element 

    let sort_method = self.value; 
    
    // Step 1: Get the current URL
    var currentUrl = new URL(window.location.href);
    
    
    // Step 2: Add or update a query parameter
    currentUrl.searchParams.set('sort_method', sort_method);
    
    // Step 3: Reload the page with the updated URL
    window.location.href = currentUrl.toString();
}
