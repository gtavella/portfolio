"""
The Database Pipeline defines steps that must all 
succeed or fail together. That's why each Database Pipeline 
works on 1 database connection. This means you can 
rollback the connection if even one step fails, or commit
the connection if all steps succeed. 

A Database Pipeline (dbp for short) exposes
a success attribute (read-only) that can be checked   
for success or failure of the entire pipeline.

The failure of a step implies the failure of the entire pipeline.
The success of a step does not imply the success of the pipeline.
For the pipeline to succeed, all steps must succeed.

In short:

1 step fails -> pipeline fails
1 step succeeds -> ?
all steps succeed -> pipeline succeeds
 
A pipeline can be composed of one step; it doesn't matter.
The point of the pipeline is not how many steps there are.
The real use of the pipeline is that it makes the logic
of steps a linear, simple two-level logic. Each step is sequential,
and is executed only if all the previous steps succeeded. 

A dbp provides an intuitive, easy to debug, scalable and 
reliable way to build sophisticated data processing, 
keeping one important thing in mind: a guaranteed 
two-level logic, and no internal state corruption.

This means: 
- only the pipeline can modify its internal state
- neither the pipeline nor the steps can modify any
other object outside of themselves
- specifically, steps can only modify the state of 
themselves

A pipeline can be customized, that is why a pipeline 
is a pipeline in the first place. A step is atomic,
standard, highly predictable. A step does not depend
on the state of the pipeline. It's the pipeline's 
responsibility to call specific steps based on its state.

A step is naive about the state of the pipeline; 
a step merely does something specific and knows nothing
about who called it or what's before, after or around it. 

If a step is not enough, a new step should be created.
If a pipelien needs a non-standard step, then a new step must be created. 
Steps cannot be internally changed just to appeal to the behavior desires 
of one or any pipeline. Thus if a pipeline needs to have
a custom step, then create a new step and do not edit
existing steps.

The behavior of a step must be extremely clear and explicit. 
Nothing must be hidden. A step cannot modify the state 
of the pipeline. The state of the pipeline can only be 
changed in the pipeline.

That's why a step only returns values. Every variable 
in a step is a local variable to that step. 

The state of the pipeline is only changed outside of the step,
never inside. A step must do one thing only. 

In this whole mechanism, there are only two layers: 
1) the pipeline
2) the step

That's it. Any problem can be found in either of the two.
No attempts should be made to create any other layer, 
for example by calling a step inside of a step. 

That would be the fastest way to lose track or corrupt internal state.
A step should do 1 thing in a very predictable and simple fashion,
and to guarantee this it should return values in a predictable format.

The pipeline should not modify the state of any other object.
The pipeline should collect its own errors, for example.


HOW IT WORKS

A dbp gets or opens a database connection. The connection 
can be from a connection pool, or an entirely new connection.



Syntax
dbp.<entity>.<action>

Example
dbp.tesserati.add()

"""

import datetime
import traceback
import sys
import signal
from datetime import datetime

import psycopg2
import psycopg2.extras
import psycopg2.pool
from psycopg2 import sql
import psycopg2.sql

from utils.emails import send_email
from utils.misc import item_exists
from utils.time import to_dates_tz

ThreadedConnectionPool = psycopg2.pool.ThreadedConnectionPool
SimpleConnectionPool = psycopg2.pool.SimpleConnectionPool
Connection = psycopg2.extensions.connection
Cursor = psycopg2.extensions.cursor
Composable = psycopg2.sql.Composable

# these are very often (not always) trivial exceptions, due to 
# forgetting something or very simple to correct bugs
# I've taken them by ascending order directly on python docs
# these are exceptions that have occured most of the time
# and i know quite well, so that's why i call them trivial,
# because i know almost instantly what is the problem
# i do not consider exceptions as trivial if i do not know
# or understand much about them, or if they reveal something 'deeper'
# on the OS level for example
# https://docs.python.org/3/library/exceptions.html#exception-hierarchy
TrivialExceptions = (
    ArithmeticError,
    AttributeError,
    LookupError,
    NameError,
    ReferenceError,
    TypeError,   
    ValueError
)


try:
    from app_env import IS_PRODUCTION
except ImportError:
    raise ImportError("you must indicate if this is a production env")


try:
    if IS_PRODUCTION:
        from app import db_conn_pool  
except ImportError:
    raise ImportError("'db_conn_pool' was not found in module 'app'")


try:
    from app_env import REAL_DATABASE_URL
except ImportError:
    raise ImportError("could not find 'REAL_DATABASE_URL'")


conn_kwargs = {
    "dsn": REAL_DATABASE_URL
    # "cursor_factory": psycopg2.extras.RealDictCursor
}
 



def _catch_exc(_instance_call):
    """
    This function decorates the __call__ method
    of a callable instance. So the inner function/wrapper
    of this decorator is called when an instance 
    is called (more details below). 
    
    This decorator registers interpreter-level and 
    OS-level exceptions that can occur as stuff is happening
    in the database, thus connection must be forcefully rolled back 
    and closed. Specifically, this decorator:
     
    1) registers an handler on SIGTERM 
    (ethis signal gets sent by OS on dynos restart, for example)
    
    2) catches KeyboardInterrupt, SystemExit and 
    at last, any other exception
    
    In ALL these cases, connection is rolled back
    and connection is closed/put back.         
    """
    
    def _pipeline_instance(self, ctx, *args, **kw):
        """
        These parameters are of the __call__ instance method.
        This makes the class instance callable.
        So this specific function (I mean, precisely this inner
        function/wrapper where this docstring belongs) 
        is called when you call the instance.
        Here's how it works: 
        ### 1) instantiate class = create instance
        c = C()
        ### 2) call instance
        c() <-- this is when __call__ is called = when this 
        inner function/wrapper is called
        """
        
        # this wraps everything from db connections,
        # pipeline function, opening cursor, executing query etc. 
        try:
            _instance_call(self, ctx, *args, **kw)
            
        except TrivialExceptions:
            self._haserror = True
            self._endconn()
            self.add_error("Problema con il database")
            self.add_ctxerror(traceback.format_exc()) 
        
        # when user presses Ctrl-C
        # default exception raised by python, equivalent to signal.SIGINT
        except KeyboardInterrupt as e:
            self._haserror = True
            self._haserror = True
            print("KeyboardInterrupt caught. Cleaning up...", flush=True)
            self._endconn()
            self.add_error("Utente e' uscito dal programma")
            self.add_ctxerror(traceback.format_exc()) 
            
        
        # when python interpreter wants to exit
        except SystemExit:
            # if done_cleaning:
            self._haserror = True
            print("SystemExit caught. Cleaning up...", flush=True)
            self._endconn()
            self.add_error("L'interprete e' voluto uscire dal programma")
            self.add_ctxerror(traceback.format_exc()) 
 
            # am i sure about this below??
            # sys.exit(0) is precisely what is raising this exception
        
        except Exception:
            self._haserror = True
            self._endconn()
            self.add_error("Problema nei server")
            self.add_ctxerror(traceback.format_exc()) 
        
        except BaseException:
            self._haserror = True
            print("Unexpected exception from BaseException was caught")
            self._endconn()
            self.add_error("Problema nei server")
            self.add_ctxerror(traceback.format_exc()) 
        
        
        if not self.success: 
            
            ctx.database_operation.messages_error.extend(self.errors)
            ctx.errors_context_str.extend(self.ctxerrors)
            
            send_email_notif_bug_when_prod(ctx) 
            
            # send_email         
            print(self.errors)
            print(self.ctxerrors[0]) 
            print(self.conn)
           
        
    return _pipeline_instance





def send_email_notif_bug_when_prod(ctx):
    """
    Send an email to developer if there was an error
    during database operations and if the app is running in production environment.
    """
    
    if IS_PRODUCTION:
        
        send_email.avviso_bug(ctx)
                
                

  
def if_error_send_email(ctx):
    """
    Send an email to developer if there was an error
    during database operations and if the app is running in production environment.
    """
    
    # if not was_ok_after_close(ctx):
                
    send_email.avviso_bug(ctx)
        
            
            
  

def log_sqlquery(func):
    """
    This is a step function
    """
    def new_func(self,
                  curs: Cursor,
                  query: str | bytes | Composable,
                  params: dict):
        
        as_string = query.as_string(self.this_pipeline.conn) # type: ignore
        
        mogrified = curs.mogrify(query, params).decode("utf-8") # type: ignore
    
        # print()
        # print("as_string")
        # print(as_string)
        print()
        print("mogrified")
        print(mogrified)    
        print()
        
        return func(self, curs, query, params)
    return new_func



class DatabasePipeline:
    """
    When working with a connection pool,
    it is assumed that the connection pool 
    will be found at app.db_conn_pool
    
    By default, each cursor has a cursor factory
    of RealDictCursor, which converts table columns 
    into dictionary keys. How is that not a must?
    """
    
    # keeps track of all the instances of this class 
    _instances = []
    
    def __init__(self, pipeline_func) -> None:
        """
        :param pipeline_func: the function that acts 
        as the pipeline function = the function that
        is being decorated
        """
        # all steps in a pipeline
        self._steps = []
        
        # add this instance so that the class 
        # can keep track of the instances that
        # are being instantiated
        type(self)._instances.append(self)
        
        self.pipeline_func = pipeline_func
        
        self.conn: Connection | None = None
        
        self.dbconn_pool: SimpleConnectionPool | ThreadedConnectionPool | None = None
        
        self._haserror = False
        # not sure if this is still relevant
        self._success = False
        
        # error messages 
        self.errors = []
        # error contexts, which is a generality for saying "error traceback"
        self.ctxerrors = []
        
        # check this attribute with getters and setters 
        # so that it has acceptable values

        if IS_PRODUCTION:
            self.conn_method = "pool" # pool, new
            # reference to this only, only set it once
            self.dbconn_pool = db_conn_pool
        else:
            self.conn_method = "new" # pool, new
    
    
    # with one single command, you know if everything went well
    # no need to check if there are errors, this is the only central place
    # to check whether the entire was successful
    @property    
    def haserror(self):
        return self._haserror
    
    @property
    def success(self):
        return not self.haserror

    
    def add_error(self, msg):
        self.errors.append(msg)
    
    def add_ctxerror(self, ctxerr):
        self.ctxerrors.append(ctxerr)
    

    @_catch_exc
    def __call__(self, ctx, *args, **kw):
        """
        This is response for calling the pipeline function
        however since it's decorated, we do some things
        to make sure it all goes well.
        """
        # print("\n\nBEGIN--------------------")

        try:
            self.conn = self._getconn()
        except psycopg2.Error:
            self._haserror = True
            self.add_error("Problema durante la connessione al database")
            self.add_ctxerror(traceback.format_exc())
        # this runs only if opening/getting a connection 
        # was successful, but that might still not be a strong enough
        # guarantee that the connection is fully ready/open to finally work with
        else:        
            if self._conn_is_open():
                self._run_pipeline_func(ctx, *args, **kw)
            else:
                self._haserror = True
                self.add_error("Problema con la connessione al database")
                self.add_ctxerror("During getting/opening of connection, "
                                    "no exception was raised, but connection is empty, "
                                    "closed or not an actual connection.")            
        
        # when you get there it's when you've finished running 
        # the pipeline function, so you must decide whether to 
        # rollback or commit the connection based on what happened 
        # inside the pipeline function, which is indicated by self.success
        self._endconn()
        # print(self.conn) 
        # print("END----------------------------\n\n")
         
          
        
    def _endconn(self):
        """
        Use this function to generically end a connection session.
        Generic function for ending a connection, which
        actually consists of rollbacking OR committing, 
        and then closing/putting back the connection.
        """
        if not self._conn_is_open():
            return False

        if not self.success:
            self._rollback()     
        else:
            self._commit()
        
        self._closeconn()
        
        
        
    def _rollback(self):
        try:
            self.conn.rollback() # type: ignore
            print("> rollback success")
        except psycopg2.Error:
            self._haserror = True
            self.add_error("Problema nel database: c'e' stato un errore interno mentre "
                        "venivano processati i tuoi dati, il sistema ha cercato di "
                        "tornare indietro, ma non "
                        "e' riuscito. Per favore controlla i dati su cui ha lavorato.")
            self.add_ctxerror(traceback.format_exc()) 
    
    
    def _commit(self):
        try:
            self.conn.commit() # type: ignore
            print("> commit success")
            
        except psycopg2.Error:
            self._haserror = True
            self.add_error("Problema nel database: non c'e' stato nessun errore "
                        "durante il processamento dei tuoi dati, tuttavia "
                        "il sistema non e' riuscito a salvare i cambiamenti. " 
                        " Per favore controlla i dati su cui ha lavorato.")
            self.add_ctxerror(traceback.format_exc()) 
    
    
    def _closeconn(self):
        """
        The semantics of close, end, put connection
        are not so refined yet. Read the docstring.
        """
        try:
            # connection pool
            if self.conn_method == "pool":
                self.dbconn_pool.putconn(self.conn) # type: ignore
            # connection previously opened
            else:
                self.conn.close()         # type: ignore
        except psycopg2.Error:
            self._haserror = True
            self.add_error("Problema nel database")
            self.add_ctxerror(traceback.format_exc())
    

    def _conn_is_open(self):
        """
        Even if getting/opening a connection
        did not raise an exception, that might not
        guarantee that everything went as it should have,
        so check that the connection is real and open and ready
        to be worked with
        """
        if self.conn is None:
            return False
        # does this work with connections from the pool as well?
        if not isinstance(self.conn, Connection):
            return False
        if self.conn.closed == 1:
            return False
        return True
        
        
    def _run_pipeline_func(self, ctx, *args, **kw):
        """
        This function will manage its own exceptions.
        Why? Because when this function is called we are 
        100% sure that the connection was either opened/gotten 
        successfully, so the "opening/getting the connection"
        part is done, and we can now confidently call the
        pipeline function to do whatever it needs to do 
        with this connection. 
        """
        
        # capture anything that might go wrong 
        # in the pipeline function
        try:
            self.pipeline_func(ctx, self, *args, **kw)
        except psycopg2.Error:
            self._haserror = True
            self.add_error("Problema nel database")
            self.add_ctxerror(traceback.format_exc()) 
        

    
    def _getconn(self):
        """
        "Get a connection" from whatever is the connection method.
        So this function acts as a generality for 
        "actually get a connection from a connection pool"
        or "open a new connection"
        """
        
        if self.conn_method == "pool":
            return self._getconn_pool()
        else:
            return self._getconn_new()
    
    
    def _getconn_pool(self):
        """
        Get a connection from a connection pool.
        """ 
    
        conn = self.dbconn_pool.getconn() # type: ignore
        # print("> [GIUSEPPE] just got a connection from the pool")
        return conn            
    
    
    def _getconn_new(self):
        """
        Open a new connection.
        """
        conn = psycopg2.connect(**conn_kwargs) # type: ignore
        # print("Opened new connection")
        return conn 
    
    
        
        


class DatabasePipelineStep(DatabasePipeline):
    
    def __init__(self, step_func):
        self.step_func = step_func
        # this is none only right at the very beginning
        # it will be immediately updated once
        # the pipeline step function is called 
        self.this_pipeline: DatabasePipeline | None = None
        
        # a list of cursors for this pipeline step
        self.cursors = []
    
    def __call__(self, 
                 this_pipeline: DatabasePipeline, 
                 params: dict[str, str|float|datetime|None]={},
                 **kw):
        """
        This function is called when you call a function
        that has been decorated with a @pipelinestep
        decorator. Before calling the actual function
        that was decorated, there's some pre-work.  
        
        example:
        
        @pipelinestep
        def add_item(step: pipelinestep, params, **kw):
            pass
        
        when you call add_item you are calling this function,
        and before calling add_item some pre/post-work is required 
        
        Each pipeline step function has complete freedom and restrictions over:
        
        1) has its own params (it's like a data template that gets
        filled in specific ways by the form sent by the user)
        2) handles its own cursors (open and automatic close also)
        3) builds its own sql query
        4) it can only modify its own environment (the pipelinestep)
        5) the input it receives are as generic as possible.
        the step function is naive about its environment,
        and cannot modify any global variable and not even the pipeline state
        6) the output format is standard and returning this standarized
         output is the only way the pipeline will ever know what happened
         (result set or anything else) inside the step function
        
        The parameters of this function are the 
        specific step_func parameters
        
        :param this_pipeline: it's the pipeline
        to mount/add this step on
        
        :param params: the dictionary which represents 
        the mapping placeholder->value to replace in the
        sql query. what's the difference between params
        and a form? why use a new term and not just stick with form? 
        the form is the actual mapping that comes directly 
        from user input, it's the actual form the user has filled out; 
        params is the form but after custom processing 
        (like converting an empty string to None, 
        lowercasing the email field, adding new fields,
        removing certain fields, translating search filter 
        fields into what they actually mean  etc.). 
        this is because it doesn't matter what the user sends,
        user input must always be filtered and reconstructed. 
        so params is the final, safe dictionary/mapping that  
        will be used both to build the sql query and 
        to replace the query named placeholders. in short,
        the conceptual separation serves to differentiate 
        "what is input that comes directly from user" from
        "the final mapping to use to build the sql query 
        and replace this sql query, after processing user input"
        
        ********************************
        to recap, journey looks like:
    
        user sends "form"           
               |
               |
        system processes form
               |
               |
        system produces "params"
               |
               |
        system builds sql query with params
               |
               |
        system executes query and replaces
        params placeholders with their values    
               |
               |
        system returns results from query     
        ********************************       
        
        :param form: it is assumed that almost any
        operation involving steps will also involve 
        a form, so it makes sense to give a first seat
        
        :param kw: additional parameters for this specifc
        step, that are implementation-dependent. the point
        is that a step should be purposefully naive about 
        its environment, because it must not affect it,
        nor do something based on it. this is another way
        of saying that a pipeline step is isolated from
        any other step, and special care must be taken 
        to ensure that each pipeline step receives input
        and gives output that is as generic and 
        as standard as possible
        """
        
        # now you know what pipeline this step belongs to
        self.this_pipeline = this_pipeline
    
        # every time you add a step with @pipelinestep,
        # you effectively add a step to the pipeline
        this_pipeline._steps.append(self)

        # call the step function passing 
        # the step instance as the only parameter
        # execute function now, store result for later
        step_result: DatabasePipelineStepResult = self.step_func(self, params, **kw)
         
        
        # THIS IS REACHED AFTER STEP FUNCTION FINISHES
        
        # once step function is done, you may
        # do post-step function actions
        # close cursors if it was previously
        # specified to do so automatically
        for curs in self.cursors:
            if curs["automatic_close"]:
                self.ccursor(curs["ref"])
            else:
                # this cursor did not want to 
                # be automatically closed
                pass
        
        # this returns whoever initially called this
        # specific step function (for example add_pizza())
        # which is assumed to be in the pipeline func's scope
        return step_result
 

    def ocursor(self, 
                as_dict=True, 
                automatic_close=True, 
                *args, **kw):
        """
        Open a new cursor
        
        this method should be called, instead of 
        opening a cursor directly from the psycopg2
        because when you call a cursor from here,
        some extra things are taken care of for you, 
        for example it sets the cursor factory such
        that you can access database columns as 
        dictionary keys, which i find super useful.
        
        :param as_dict: if you do want to access database
        columns as dictionary keys, you can leave it as
        default, or you can simply disable it with as_dict=False
        
        :param automatic_close: automatically close this cursor
        when you finish this pipeline step. defaults to true
        
        if you want to pass any additional parameters 
        that you'd pass when creating a cursor, you can do so,
        just add the parameters like you would do when 
        creating a normal cursor and they will be directly
        passed to the function that creates the cursor 
        """
         
        # print("opening cursor")
                
        more_params = {}
        
        # *******
        
        if as_dict:
            more_params.update({
                "cursor_factory": psycopg2.extras.RealDictCursor
            })
                
        # you can add more conditions here before updating kw with more_params
        
        # *******
        
        # final step - merge any additional/custom parameters into
        # the final parameters that will be passed to 
        # open the cursor
        kw.update(more_params)    
        
        new_cursor = self.this_pipeline.conn.cursor(*args, **kw)   # type: ignore
        
        # add this new cursor to the list of cursors
        # for this step
        self.cursors.append({
            "ref": new_cursor,
            "automatic_close": automatic_close
        })
        
        # this should work both when opening a new connection,
        # as well as when getting a connection from the pool
        return new_cursor
    
    
    def ccursor(self, curs: Cursor):
        """
        Close the cursor
        """
        # print("closing cursor manually")
        # print(curs)
        # close the cursor only if's already open
        if curs.closed == 0:    
            curs.close()
            # print("cursor automatically closed")
        else:
            # cursor.closed is either 1 (closed) 
            # or -1 (unknown). currently investigating
            # on what's the meaning of -1
            # however psycopg2 raises a psycoph2.InterfaceError
            # stating that the cursor is already closed
            # when trying to fetch data with it
            # so not sure 
            pass
        
      
     
    @log_sqlquery
    def execquery(self,
                  curs: Cursor,
                  query: str | bytes | Composable,
                  params: dict):
        """
        Execute the query with these params on this cursor
        
        Does exactly what cursor.execute(query, query_params) 
        does with psycopg2. This function only adds
        some nice features like logging the query string
        and mogrified query, also for easier debugging 
        """
           
        return curs.execute(query, params)
        
    
    
    def execmquery(self):
        """
        Execute many the query.. cursor.executemany(...)
        not implemented yet
        """
        pass
     
     
    def result(self, *args, **kw):
        """
        Creates a step result instance. 
        This provides standardized access to step results
        and any additional information that each 
        step result might want to communicate to the
        pipeline function
        
        For reference on standard parameters, see 
        DatabasePipelineStepResult constructor
        """
        return DatabasePipelineStepResult(self, *args, **kw)
     

    def __str__(self): 
        return f"<pipeline step '{self.step_func.__name__}'>"
     




class DatabasePipelineStepResult(DatabasePipelineStep):
    """
    Define a common, standard result for a pipeline step 
    """
    def __init__(self, 
                 this_step: DatabasePipelineStep, 
                 items: list=[],
                 *args,
                 **kw):
        """
        :param this_step: the function step this is called from
        :param items: results returned from database. always a list,
        even if it's one element. this is to keep consistency
        :param pagin: if this result uses/is involved in the pagination
            mechanism. usually SELECT statements may involve the pagination
            mechanism. defaults to false
        
        define in this constructor all attributes that
        you want to make available to the pipeline function 
        when step function returns 
        """        
    
        self.items = items
        self.item_exists = item_exists(items)
        self.pagin_last = None
        
        # I'm interested  in the pagination mechanism
        # only when it's a select statement. 
        # in all other cases ignore it? 

        if item_exists(items): 
            # automatically converts the pagination 
            # last created_at so that I can send it via 
            # internet as a string  
            # it works if the ordering of the items is 
            # in descending order (by most recent)
            
            # the items/item might not always be
            # coming from SELECT statements. 
            # so make sure that the key is present
            if "created_at" in items[-1]:        
                self.pagin_last = to_dates_tz(items[-1]["created_at"]) 
             
         
         

    def __str__(self): 
        return f"<pipeline step result>"
 

 
def _handle_sigterm(signal_number, frame):    
    print("Received SIGTERM, cleaning up...", flush=True)        
    for inst in DatabasePipeline._instances:
        inst._endconn()
    sys.exit(0)
    
    

# bug fix: ValueError: signal only works in main 
# thread of the main interpreter??
def _handle_sighup(signal_number, frame):    
    print("Received SIGHUP, cleaning up...", flush=True)
    for inst in DatabasePipeline._instances:
        inst._endconn()
    sys.exit(0)
            
        



"""
First thing, register an handler for termination signal
this must go here because the handler should be registered
only when the pipeline function is actually called 
pipeline function = instance.__call__
if this signal handler is triggered, it means
a SIGTERM signal was sent precisely when the pipeline
function was s
"""

signal.signal(signal.SIGTERM, _handle_sigterm)

try:
    # it seems like on windows SIGHUP is not supported
    # in that case do not register the signal handler
    signal.SIGHUP # type: ignore
    signal.signal(signal.SIGHUP, _handle_sighup) # type: ignore
except AttributeError:
    pass



pipeline = DatabasePipeline

pipelinestep = DatabasePipelineStep
