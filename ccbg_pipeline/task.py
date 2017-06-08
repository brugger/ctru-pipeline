##
# 
# 
##

from __future__ import print_function, unicode_literals

import pprint as pp

DEBUG = 0


class task_status( object ):
    FINISHED    =    1
    FAILED      =    2
    RUNNING     =    3
    QUEUEING    =    4
    RESUBMITTED =    5
    SUBMITTED   =    6
    KILLED      =   99
    UNKNOWN     =  100


class task(object):

    status   = task_status.SUBMITTED
    active   = True
    command  = None

    output_file= None
    limit      = None
    logic_name = None
    pre_task_ids = None
    delete_file = None
    thread_id = None
    cmd       = None

    def __init__(self,  cmd = cmd, logic_name=logic_name, limit=None, delete_file=None, thread_id=None):

        self.cmd = cmd
        self.logic_name = logic_name

        if ( limit is not None ):
            self.limit = limit

        if ( delete_file is not None ):
            self.delete_file = delete_file

        if ( thread_id is not None ):
            self.thread_id = thread_id


    def set_status( self, status=None ):
        if ( status is not None ):
            self.status = status


class Step ( object ):

    name      = None
    function  = None
    cparams   = None
    step_type = None
    pipeline  = None

    def __init__( self, pipeline, name, function, cluster_params=None, step_type=None):
        self.pipeline       = pipeline
        self.name           = name
        self.function       = function
        self.cluster_params = cluster_params
        self.step_type      = step_type




    def __getitem__(self, item):
        
        if ( item.startswith("_")):
            raise AttributeError

        try:
            return getattr(self, item)
        except KeyError:
            raise AttributeError


    def __setitem__(self, item, value):

        if ( item.startswith("_")):
            raise AttributeError
        
        try:
            return setattr(self, item, value)
        except KeyError:
            raise AttributeError



    # Generic step adder, wrapped in the few functions below it
    def add_step( self, ):  
        return self.pipeline.add_stepp( self.name, function, name=None, cluster_param = None, step_type = None )

    def next(self, function, name=None, cluster_param=None):
        return self.pipeline.add_step( self.name, name, function, cluster_param);

    def merge(self, function, name=None, cluster_param=None):
        return self.pipeline.add_step( self.name, name, function, cluster_param, step_type='sync');

    def thread_merge(self, function, name=None, cluster_param=None):
        return self.pipeline.add_step( self.name, function, name, cluster_param, step_type='thread_sync');

    def __repr__(self):
        return "{name}".format( name=self.name )



class task_list( object ):

    # Overall task list
    tasks = {}

    # tasks that have not yet been submitted to the queue/system
    task_list = []

    task_id = 1

    def __init__(self, ):
        pass



    def next_task_id():
        task_id += 1

        return task_id


class thread( object) :
    pass

class threads( object ):
    thread_counter   = 0 # This is for generating internal thread_id 

    def next_id():
        thread_counter += 1
        return thread_counter
    pass

class Pipeline( object ):


    _project_name = "CCBG" 
    _queue_name   = ""
    _project_id   = ""

    
    # For housekeeping to see how long the processign took
    _start_time = None
    _end_time   = None

    # when was the run information last saved
    _last_save      =   None
    # How often to save, in secs
    _save_interval  = 300

    _max_retry      =   3
    _failed_tasks   =   0 # failed jobs that cannot be restarted. 


    _sleep_time     =   30
    _max_sleep_time =  300
    _sleep_start    =  _sleep_time
    _sleep_increase =   30

    # to control that we do not flood the cluster, or if local block server machine.
    # -1 is no limit
    _max_jobs       =  -1 

    _current_logic_name = None
    _pre_jms_ids    = None
    _use_storing    =   1 # debugging purposes
    _argv = None  # the argv from main is fetched at load time, and a copy kept here so we can store it later
    _freeze_file = None
    
    _restarted_run  =   0
    
    
    _delete_files = []
    
    _analysis_order = {}

    _step_dependencies = {}

    _cwd      = "./"

    _analysis = {}
    _flow     = {}

    _start_steps = []


#    def __getitem__(self, item):

#        if self.hasattr( item ):
#            return self.item

    # This is mainly for writing nice error messages and to see in the
    # cluster software who is responsible for running things
    def project_name(self, new_name = None ):
        if new_name is not None:
            self._project_name = new_name
        return self._project_name

    # who to charge when submitting jobs
    def project(self, new_name = None ):
        if new_name is not None:
            self._project = new_name
        return self._project

    # default queue to run on
    def queue_name(self, new_name = None ):
        if new_name is not None:
            self._queue_name = new_name
        return self._queue_name

    def save_interval(self, new_interval ):
        if ( new_interval is not None ):
            self._save_interval = int(new_interval)
        return self._save_interval

    def max_retry(self, max_nr = None):

        if ( max_nr is not None ):
            self._max_retry = int( max_nr )
        return self._max_retry

    def sleep_time(self, new_sleeptime=None):
        if new_sleeptime is not None:
            self._sleep_time = int( new_sleeptime )
        return self._sleep_time

    def max_sleep(self, new_sleeptime=None):
        if new_sleeptime is not None:
            self._max_sleep_time = int( new_sleeptime )
        return self._max_sleep_time

    def sleep_increase(self, new_increase=None ):
        if new_increase is not None:
            self._sleep_increase = int( new_increase )
        return self._sleep_increase

    def max_jobs(self, new_max = None ):
        if new_max is not None:
            self._max_jobs = int (new_max )
        return self._max_jobs



    def start_step(self, function, name=None, cluster_params = None):


        if name is None:
            name = function.__name__

        start_step = Step( pipeline = self,
                           name = name, 
                           function = function)

        
        self._start_steps.append( start_step )

        self._analysis[ name ] = start_step
        
        return start_step


    # Generic step adder, wrapped in the few functions below it
    def add_step( self, prev_step, function, name=None, cluster_param = None, step_type = None):

        if name is None:
            name = function.__name__
    
        if ( step_type is not None ):
            self._analysis[ name ][ step_type ] = 1
            print("Step type: {}-{} type:{}".format(prev_step, name, step_type))
            pp.pprint ( self._analysis )

        step = Step( pipeline = self,
                           name = name, 
                           function = function, 
                           step_type = step_type)

        self._analysis[ name ] = step

        if ( prev_step not in self._flow ):
            self._flow[ prev_step ] = []

        self._flow[ prev_step ].append( name )

        return step


    # Simple wrapper functions for the add_step function above.
    def next_step(self, prev_step, function, name=None, cluster_param=None):
        return self.add_step( prev_step, name, function, cluster_param);

    def global_merge_step(self, prev_step, function, name=None, cluster_param=None):
        self.add_step( prev_step, name, function, cluster_param, 'sync');

    def thread_merge_step(self, prev_step, function, name=None, cluster_param=None):
        self.add_step( prev_step, name, function, cluster_param, 'thread_sync');


    def print_flow(self, starts = None ):

        pp.pprint( self._flow )
        pp.pprint( self._analysis )

        if ( starts is None ):
            starts  = self._start_steps

        if (  starts is None):
            print( "No start step(s) defined")
            exit()

        analyses = []

        for start in starts:
            self.set_analysis_dependencies( start )

        logic_names = starts
        
        pp.pprint ( logic_names )

        print( "Starting with: {} \n".format( starts ))
        print( "--------------------------------------------------\n")
        while logic_names:

            current_logic_name = logic_names.pop()
            print( "{} queue: [{}]".format( current_logic_name.name, logic_names))
  
            analyses.append( current_logic_name )

            if current_logic_name.name not in self._analysis:
                print( "No information on {} in the analysis dict\n".format( current_logic_name ))
                exit()
            else:
                function = self._analysis[ current_logic_name.name ][ 'function' ]


            next_logic_names = self.find_next_step( current_logic_name )
      

            if ( len(next_logic_names) ):
      
                for next_logic_name in next_logic_names:

      
                    if self._analysis[ next_logic_name ][ 'sync' ]:
                        print( "{} --> {} (Synced!!)\n".format( current_logic_name, next_logic_name))
                    elif self._analysis[ next_logic_name ][ 'tread_sync' ]:
                        print( "{} --> {} (Thread_synced!!)\n".format( current_logic_name, next_logic_name))
                    else:
                        print( "{} --> {}\n".format( current_logic_name, next_logic_name))

                    if ( waiting_for_analysis(next_logic_name, analyses)):
                        pass
                    else:
                        logic_names.append( next_logic_name )

        print( "--------------------------------------------------\n")




    def find_next_step(self, step_name ):
  
        res = []

        if step_name not in self._flow:
            return res        

        next_step = self._flow[ step_name ]

        if type( next_step ) is 'list':
            res.append( set(next_step) )
        else:
            res.append( next_step )
  
        return res



    def set_analysis_dependencies(self,  step_name ):


        step_names = self.find_next_step( step_name )
        
        for next_step in step_names:
            self._step_dependencies[next_step_name].append( step_name )
            
        while ( step_names ):
            step_name = step_names.pop()
            
            next_step_names = self.find_next_step( step_name );

            if ( not next_step_names ):
                continue

            if (  next_logic_names ):
                step_names.append( next_step_names )

            for next_step_name in next_step_names:
                self._step_dependencies[ next_step_name].append( step_name )
                    
                if step_name in self._step_dependencies:
                    self._step_dependencies[ next_step_name ].append( self._step_dependencies[ step_name ] )

            # make sure a logic_name only occurs once.
            #my %saw;
            #@{$dependencies{ $next_logic_name }} = grep(!$saw{$_}++, @{$dependencies{ $next_logic_name }});


