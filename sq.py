"""
:param form: the form to 
be careful with the semantics of form, query params etc.
a new term should be introduced representing the
mapping field -> value
"""


from psycopg2 import sql
from utils.DatabasePipeline import Composable, pipeline, pipelinestep
from utils.time import get_now_timestamp




def select(table: str,
           by: str,
           fields: list | None = None,
           how_many: int | None = None,
           only_one =  False,
           but=[]):
    """
    :param table: database table 
    :param fields: fields to include
    :param by: the identifier: "id", "id_ass", "email"
    when such a query is constructed, it makes sense to limit the
    result set to 1, and to not have any conditions, because 
    the identifier is all the conditions needed
    :param how_many: the max number of rows to return
    :param only_one: shortcut for LIMIT 1, default to false.
    in case :param how_many and :param only_one are both specified,
    only_one is the most specific so it is evaluated 
    so how_many=1 is equivalent to only_one=True
    :param but: fields to exclude
    
    If no fields are provided, all fields will be selected.
    """
    
    base_query = selectf(fields, but) + fromwh(table, by)
    
    # only_one has precedence over limit, in case both are specified.
    # because only_one is more specific
    if only_one:
        final_query = base_query + limt(1)
    elif how_many is not None:
        final_query = base_query + limt(how_many)
    else:
        final_query = base_query

    return final_query





 
def insert(table: str,
           params: dict, 
           but: list=[]):
    
    """
    Insert ALL fields of this form.
    
    Default behavior: no fields will be excluded unless you 
    say so explicitly with :param but
    """
    
    query = sql.SQL(""" 
        INSERT INTO {table} 
        ( {fields} )
        VALUES
        ( {placeholders} )
    """)
    
    # because function .keys returns a dict_keys type,
    # so cast it to list 
    fields = list(params.keys())
    
    return query.format(
        table=sql.Identifier(table),
        fields=filds(fields, but),
        placeholders=placeholds(fields, but)
    )

    


def update(table: str,
           ident: str,
           params: dict, 
           but=[]):
    """
    :param form: the form to update. note: by default
    the fields :param ident and "created_at" will be excluded,
    whether you say so explicitly in :param but or not.
    this is to prevent you from accidentally updating
    an item id, when you wanted to say was actually 
    "I want to update this item by its :param ident, but I do NOT
    want to update its id" so this is the default behavior:
    automatically excluding the fields :param ident and "created_at".
    if you want to exclude other fields you need to 
    explicitly say so in :param but
    
    if in the form you include fields that should not be updated,
    and you do not explicitly exclude them, it's your problem.
    because the system cannot determine which fields should or 
    should not be updated. for example if you have a secondary id
    or whatever else that you do include in the form, that is NOT
    used as the primary identifier (which is :param ident) then
    everything you include in the form will be updated UNLESS 
    you explicitly say in :param but that you do not want this
    field to be updated
    
    to recap: whatever is the identifier that you want to update 
    this item by, precisely that identifier (and field "created_at") 
    will be excluded from the updates 
    
    
    
    If you use id="id_ass" that means you want to
    update ALL elements by association id.
    Are you sure? Careful: this should be used only when you
    want to update the association itself.
    """
    
    query = sql.SQL(""" 
        UPDATE {table} 
        SET {clauses}
        WHERE {ident_field} = {ident_placeholder}        
    """)
    
    # these are standard fields that cannot be 
    # updated, so this prevents unwanted updates
    # so whatever identifier you provide, it will be 
    # excluded by the updates
    but.extend([ident])
       
    fields = list(params.keys())
        
    return query.format(
        table=sql.Identifier(table),
        clauses=fieldseq(fields, but),
        ident_field=sql.Identifier(ident),
        ident_placeholder=sql.Placeholder(ident)
    )
 




def delete(table: str,
           ident: str):
    """
    Be careful when using this. 
    Make sure the identifier is correct ("id", "id_ass", "email")
    """
    
    query = sql.SQL(""" 
        DELETE FROM {table} 
        WHERE {ident_field} = {ident_placeholder}
    """)
    
    return query.format(
        table=sql.Identifier(table),
        ident_field=sql.Identifier(ident),
        ident_placeholder=sql.Placeholder(ident)
    )

 
 
def pagin_cond(params: dict,
               quals: list[str] = [],
               how_many=5):
    """
    Pagination conditions.
    Chooses the pagination based on the sorting method.
    Use this when the pagination technique/method
    depends on the sorting method, and you do not
    know the sorting method. 
    
    :param quals: list of qualifiers for the field identifier "created_at".
    these identifiers are prepended to the final field identifier "created_at"
    """
        
    # these are passed to both the sql pagination function builders
    _kw = {
        "params": params,
        # add created_at as the last field identifier
        "fi_ident": quals + ["created_at"],
        "how_many": how_many
    }
    
    return (pagin_yng(**_kw) if (params["sort_method"] == "desc") else pagin_old(**_kw))
 
 
 
  
def pagin_yng(params: dict,
               fi_ident: str | list[str],
               how_many=5):
    """
    Pagination conditions for "most recent" (youngest).
    Starts with most recent, gives back the oldest.
    
    
    n. page:            ...    3       2       1    
    data:                   |-----| |-----| |-----| 
     
    TIME ------------------------------------------->
    
         <---- older                   younger ---->


    mind trick: youngest is not youngest in age, 
    but the greatest in timestamp. 

    filters next result set based on 
    last creation date
    
    limits result set to how_many
    
    note: for pagination to work correctly,
    sorting/filtering criteria must 
    remain consistent 
    
    note: how_many does not alter the pagination 
    mechanism. as long as the LAST creation date 
    was saved and is the correct one, you could
    also limit this result set to a new N that is different
    from the previous N. for example if the
    previous result set was capped to 30, now
    i can cap it to 50 etc. 
    
    as long as 
    1) the sorting/filtering criteria are consistent
    2) the fields to which this sorting/filtering is applied
    are the same
    3) the last creation date (if this is the specific mechanism
    in use) is correctly saved and represents the creation 
    date of the last element of the result set, after the sorting
     
    ..then you are free to cap the result set by how_many you wish
    
    pagination query must by definition have a LIMIT clause, 
    so be careful not to include this elsewhere before the final query 
    
    the first "page" does not have have a last pagination info,
    so no pagination condition is applied. but the following pages
    will have a pagination condition  applied
    
    returns:
    
    either: (for any page after and including the 2nd page) 
    
    AND "created_at" < %(pagin_last)s
    
    ORDER BY "created_at" DESC
    
    LIMIT {how_many}
    
    or: (for 1st page only)
    
    ORDER BY "created_at" DESC
    
    LIMIT {how_many}
    """
    
                

    # you can interpret this as the base case,
    # or the first page, when there's still no 
    # pagination condition precisely because
    # it's the first page
    query = (
          ordby(fi_ident, "DESC") 
        + limt(how_many)
    )
    
    # note that the condition is prepended to the query, 
    # because it goes first   
    if "pagin_last" in params:
        query = and_lt(fi_ident, "pagin_last") + query
    
    return query
 
 
 
 
def pagin_old(params: dict,
              fi_ident: str | list[str],
              how_many=5):
    """
    Pagination conditions for "oldest". 
    Starts with oldest, gives back the most recent (youngest).
    
    
    n. page:    1       2       3    ... 
    data:    |-----| |-----| |-----| 
     
    TIME ------------------------------------------->
    
         <---- older                   younger ---->


    mind trick: oldest is not oldest in age, 
    but the smallest in timestamp. 

    returns:
    
    either: (for any page after and including the 2nd page) 
    
    AND "created_at" > %(pagin_last)s
    
    ORDER BY "created_at" ASC
    
    LIMIT {how_many}
    
    or: (for 1st page only)
    
    ORDER BY "created_at" ASC
    
    LIMIT {how_many}

    """
    
    query = (
          ordby(fi_ident, "ASC") 
        + limt(how_many)
    )
    
    # note that the condition is prepended to the query, 
    # because it goes first   
    if "pagin_last" in params:
        query = and_gt(fi_ident, "pagin_last") + query
    
    return query
    




def filds(fis: list, 
               but=[]):
    
    """
    "field1", "field2", "field3" ...
    
    used in SELECT and INSERT
    
    *** generic example ***
    
    SELECT {fields}  <-- here 
    
    FROM {table}
    
    *** generic example ***
    
    INSERT INTO {table}
    
    ( {fields} )   <-- here 
    
    VALUES
    
    ( {placeholders} )
    
    """
    
    return sql.SQL(", ").join(
        [sql.Identifier(fi) for fi in fis if fi not in but]
    )




def placeholds(fis: list, 
                     but=[]):
    
    """
    %(field1)s, %(field2)s, %(field3)s ...
    
    used in INSERT
    
    *** generic example ***
    
    INSERT INTO {table}
    
    ( {fields} )      
    
    VALUES
    
    ( {placeholders} )   <-- here
    
    """
    
    return sql.SQL(", ").join(
        [sql.Placeholder(fi) for fi in fis if fi not in but]
    )




def fieldseq(fis: list, 
             but=[]):
    """
    fields are equal to
    
    used in SET
    
    returns:
    
    "field1" = %(field1)s 
    
    ,
    "field2" = %(field2)s  
    
    ,
    "field3" = %(field3)s ...
    """
    return sql.SQL(", ").join(
        [fieldeq(fi) for fi in fis if fi not in but]
    )
    


def fieldeq(fi: str):
    """
    field identifier is equal to field placeholder
    
    returns:
    
    "field" = %(field)s  
    """
    
    query = sql.SQL("""
        {field} = {placeholder}  
    """)
    
    return query.format(
        field=sql.Identifier(fi), 
        placeholder=sql.Placeholder(fi)
    )
    
    


def selectf(fields: list | None = None, 
            but=[]):
    """
    select all fields or specific fields
    
    returns:
    
    SELECT *
    
    or 
    
    SELECT "myfield1", "myfield2", "myfield3" ...
    
    If no fields are specified, default behavior is
    all fields will be selected 
    """

    query = sql.SQL("""
        SELECT {fields}
    """)
    
    return query.format(
        fields=filds(fields, but) if fields is not None else sql.SQL(" * ")
    )
    
    

    


def fromwh(table: str, 
           field: str):
    """
    from where
    
    used in SELECT
    
    returns: 
    
    FROM "mytable"
    
    WHERE "myfield" = %(myfield)s
    """
    
    query = sql.SQL("""
        FROM {table}
        WHERE {field} = {placeholder}
    """)

    return query.format(
        table=sql.Identifier(table),
        field=sql.Identifier(field),
        placeholder=sql.Placeholder(field)
    ) 




def limt(n: str | int):
    """
    LIMIT {n} 
    
    n is directly replaced like a literal, it's an integer
    
    example:
    
    LIMIT 26
    
    example: 
    
    LIMIT 1
    
    returns:
    
    LIMIT myn
    """
    
    query = sql.SQL("""
        LIMIT {n}
    """)
    
    return query.format(
        n=sql.SQL(str(n))
    )
 



def and_fildeq(fi):
    """
    and field identifier is equal to field placeholder 
    
    returns:
    
    AND "field" = %(field)s  
    """
    
    return (sql.SQL("""
        AND {}
    """).format(fieldeq(fi))) 
         
 
 
def and_fildeq_cond(params: dict, 
                    fi: str):
    """
    and conditional: field identifier is equal to field placeholder 
    meaning: check first if the field is in params, 
    if it's not there do not it to the sql query
    
    returns:
    
    AND "field" = %(field)s  
    """
    
    if fi not in params:
        return sql.Composed([])
    
    return (sql.SQL("""
        AND {}
    """).format(fieldeq(fi))) 
         
 

def and_tpatt(fi_ident: str,
              fi_placehold_patt: str):
    """
    and field identifier ilike field placeholder pattern
    
    field placeholder pattern must already contain 
    the valid pattern to match against field identifier;
    no alterations or special considerations are
    made in this function
    
    AND {fi_ident} ILIKE {fi_placehold}
    """
    
    query = sql.SQL(""" 
        AND {fi_ident} ILIKE {fi_placehold_patt} 
    """) 
    
    return query.format(
        fi_ident=sql.Identifier(fi_ident),
        fi_placehold_patt=sql.Placeholder(fi_placehold_patt)
    ) 




def and_yeard_eq(fi_ident: str,
                 fi_placehold: str):
    """
    and year from field identifier equals year 
    of field placeholder
    
    use this when you want to select items whose 
    column name is fi_ident and it's of (database) type DATE, 
    and you want to extract the year from this column
    and compare it against the fi_placehold, which is a
    named placeholder that will be replaced when executing
    the query
    
    :param fi_ident: the field identifier = database column name
    that needs to be equal to whatever is specified in the
    dynamic field field placeholder.
    
    so field identifier is the "static" database field, 
    and field placeholder keeps the same name of course
    but it's its value that changes  
    
    example:
    an entrata has a column name (field identifier) "data_pagamento".
    you want to find entrate whose data_pagamento is equal 
    "anno_esercizio" (field placeholder). so the logic is:
    
    where ... "data_pagamento" = %(anno_esercizio)s
    
    this is equivalent to saying "select entrate 
    whose column data_pagamento is equal to whatever
    value i specify in a dynamic field called anno_esercizio"

    now more generically:
    
    "select items whose column field identifier is equal to
    whatever i specify in a dynamic field called field placeholder"

    returns: 
    
    AND EXTRACT( YEAR FROM "myfield_ident" ) = %(myotherfield_placehold)s
    """
    
    query = sql.SQL("""
        AND EXTRACT( YEAR FROM {fi_ident} ) = {fi_placehold}
    """)
    
    return query.format(
        fi_ident=sql.Identifier(fi_ident),
        fi_placehold=sql.Placeholder(fi_placehold)   
    )




def and_gt(fi_ident: str | list[str], 
           fi_placehold: str):
    """
    and field identifier is greater than field placeholder
    
    the item is selected or the query returns true if 
    what's on the left is greater than what's on the right: 
    what's on the right is the LOWER BOUND
    
    if you're using dates, fi_placehold can be interpreted 
    as the "lower bound" of fi_ident, and the item is selected
    if field identifier (database column of that item) is greater than
    field placeholder (the parameterized value representing 
    user input)
    
    it's a generic query: field identifier could be exactly 
    the same as field placeholder, however it's not assumed 
    
    ## how it works
    take an item's creation date. example: 

         date_creation_start       date_creation
    |              | >>>>>                |
    |              | / / / / / / / / / /  | / / / / / / / /
    | time --------o----------------------o----------------->
    |              | / / / / / / / / / /  | / / / / / / / /
    |              | >>>>>                |
    
    in this case, is the date of creation (the field identifier) 
    greater than the the date of creation start (the field placeholder)? 
    YES, so this item will be selected
    
    use this query when you want the value of fi_placehold
    to act as a "lower bound" for fi_ident. when you want
    to select items whose fi_ident is greater than fi_placehold,
    for example finding the items whose "date of creation" is greater
    than the "begin date". such a use effectively means 
    establishing "begin date" as lower bound of "date of creation",
    because items whose date of creation is greater than 
    begin date will be selected 

    
    :param fi_ident: field identifier = database column name
    
    :param fi_placehold: field placeholder = field name \
    of the form/mapping from user input. the value that \
    the user has sent will replace this placeholder when \
    executing the query
    
    example:
    
    the user has documents of type A with a date_of_creation
    the user wants to find documents whose date_of_creation
    is AFTER a certain date, "give me documents of type A"
    whose date_of_creation is after a certain point" is what 
    the user wants. this certain date is the 
    "data_of_creation_start" user field. 
    so how would you use this query?
    
    AND "date_of_creation" > %(date_of_creation_start)s 
     
    why? because "date_of_creation" is the database column
    name that documents of type A have. %(date_of_creation_start)s
    is the placeholder where the actual "date_of_creation_start"
    field from user input will be replaced. so "date_of_creation_start"
    will be found in the form the user sends, which happens to 
    be search filters. why use a placeholder?
    to sanitize the input and make the query generic
    
    example:
    
    AND "data_documento" > %(data_inizio_documento)s
    
    this translates to "select the documenti whose 
    data_documento is greater than the specified data 
    inizio documento" 
    
    a specific example of this is "the user is working
    with documenti, and wants to filter these documenti 
    by data inizio documento. then they click 'Filter documenti'. 
    with this query, the system finds the documenti whose 
    "data_documento" is greater than whatever the user
    has picked as  %(data_inizio_documento)s, which is 
    a placeholder precisely because this value will be 
    replaced, since it's a literal that comes from user input. 
    "
    
    returns:
    
    AND "myfield1" > %(myfield2)s
    """
        
    return sql.SQL(""" 
        AND {fi_ident} > {fi_placehold} 
    """).format(
        fi_ident=_qual_ident(fi_ident),
        fi_placehold=sql.Placeholder(fi_placehold)
    )
    
    
    

def and_lt(fi_ident: str | list[str], 
           fi_placehold: str):
    """
    and field identifier is less than field placeholder
    
    the item is selected or the query returns true if 
    what's on the left is less than what's on the right: 
    what's on the right is the UPPER BOUND
    
    works exactly like :func and_gt, but opposite logic
    (not exactly opposite because the opposite of < is >= not >)
    
    for details see :func and_gt
    
    if you're using dates, fi_placehold can be interpreted 
    as the "upper bound" of fi_ident
    
    take an item's creation date. example: 

                date_creation       date_creation_end
    |              |                <<<<< |
    |              | / / / / / / / / / /  | / / / / / / / /
    | time --------o----------------------o----------------->
    |              | / / / / / / / / / /  | / / / / / / / /
    |              |                <<<<< |
    
    in this case, is the date of creation (the field identifier) 
    less than the the date of creation end (the field placeholder)? 
    YES, so this item will be selected
    
    returns: 
    
    AND "myfield1" < %(myfield2)s
    """
    
    return sql.SQL("""
        AND {fi_ident} < {fi_placehold} 
    """).format(
        fi_ident=_qual_ident(fi_ident),
        fi_placehold=sql.Placeholder(fi_placehold)
    )
     


def ordby(fi_ident: str | list[str], 
          how: str):
    """
    order by field identifier how
    
    example:
    
    ORDER BY "date_birth" ASC
    
    returns:
    
    ORDER BY "myfield" DESC
    """
    
    query = sql.SQL("""
        ORDER BY {fi_ident} {how}
    """)
     

    return query.format(
        fi_ident=_qual_ident(fi_ident),
        how=sql.SQL(how)
    )



        
    
"""
there are some kinds of "standard time filters":
- last N [hours|days|weeks|months|years], where N >= 0 and N is [int|float]
- this [day|week|month|year] 

and:
- they are all generic times in the past.
- the output format is [date string|timezone-aware timestamp datetime object]
- the condition to impose is always that the field identifier must be 
greater than or equal the past time computed, with "past time" being
the difference between now and whatever is that past time
- it can be only one of either last N .. or this .. for example, the 
standard time filter can only be last 7 days, or this month, or this week,
or last 30 days etc. but only one at a time


"""


def and_gte(fi_ident: str | list[str], 
           fi_placehold: str):
    """
    and field identifier is greater than or equal to field placeholder
    
    the item is selected or the query returns true if 
    what's on the left is greater than or equal to what's on the right: 
    what's on the right is the LOWER BOUND INCLUDED
    
    use before the pagination conditions
    
    see :func and_gt for more details
    
    returns:
    
    AND "myfield1" >= %(myfield2)s
    """
    
    return sql.SQL(""" 
        AND {fi_ident} >= {fi_placehold} 
    """).format(
        fi_ident=_qual_ident(fi_ident),
        fi_placehold=sql.Placeholder(fi_placehold)
    )



def and_created_after(params: dict,
                      quals: list[str] = []):
    """
    if created_after is in params, add the 
    condition: the "created_at" field identifier
    is greater than or equal to the "created_after"
    field placeholder in params. 
    otherwise it adds an empty sql string, which
    has no effect and thus leaving the 
    whole query unchanged   
    """
    
    # it works like this: since i can also pass the qualifiers
    # for this field identifier (created_at) i pass whatever 
    # qualifiers i've passed (or an empty list) to which 
    # the field identifier "created_at" is appended 
    if "created_after" in params:
        return and_gte(quals + ["created_at"], "created_after")
    else:
        # empty Composed just to make sure 
        # the return value is always Composed 
        # and not a mixed return value SQL | Composed
        return sql.Composed([])  
       
 
  

def and_contains(params: dict,
                 *fis_ident,
                 quals: list[str] = []):
    """
    and any of field identifiers contain the text/substring
    defined in the field "contains" in params
    """
    
    # if the field is not even in params,
    # then just leave the sql query unchanged
    if "contains" not in params:
        return sql.Composed([])
    
    query = sql.SQL(""" 
        AND ( {clauses} )
    """)
    
    
    # this simply produces a sort of "list" of
    # statements for each field identifier 
    # note that if any field qualifiers are provided,
    # it will be the same field qualifiers 
    # for all the field identifiers for which 
    # you want to match the pattern contains
    clauses = ([sql.SQL("{} ILIKE {}").format(_qual_ident(fi, quals), 
                                       sql.Placeholder("contains")) for fi in fis_ident])
    
    return query.format(
        # all the pattern matching statements 
        # are now joined by an OR, so that 
        # the entire sql query matches only if one of these 
        # patterns matches the field identifiers
        clauses=sql.SQL(" OR ").join(clauses)
    )




def _qual_ident(fi_ident: str | list[str], 
               quals: list[str] = []) -> Composable:
    """
    qualify field identifier
    
    :param fi_ident: the field identifier, in the sense,
    the field identifier that is LAST in the last of identifiers
    
    :param quals: the list of identifiers that are prepended
    to fi_ident 
    
    note that despite the order of parameters, the actual order
    in which the identifiers are produced is:
    first qualifiers, last fi_ident
    """
    
    # if field identifier is a string, make a list out of it, 
    # then concatenate it 
    if isinstance(fi_ident, str):
        fis_ident = quals + [fi_ident]
    # if field identifier is a list, concatenate it 
    else:
        fis_ident = quals + fi_ident
    
    # now fis_ident (field identifiers) contains the 
    # field identifiers to build the "concatenated field identifiers"
    # whose purpose is to fully qualify an identifier
    # the goal is to have a common format: a list of identifiers
    return sql.Identifier(*fis_ident) 

    
