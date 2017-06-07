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



    # Generic step adder, wrapped in the few functions below it
    def add_step( self, ):
    
        return self.pipeline.add_stepp( self.name, name, function, cluster_param = None, step_type = None )

    def next_step(self, name, function, cluster_param=None):
        return self.pipeline.add_step( self.name, name, function, cluster_param);



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

    _cwd      = "./"

    _analysis = {}
    _flow     = {}

    _start_steps = []

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



    def start_step(self, name, function, cluster_params = None):


        start_step = Step( pipeline = self,
                           name = name, 
                           function = function)
        
        self._start_steps.append( start_step )
        
        return start_step


    # Generic step adder, wrapped in the few functions below it
    def add_step( self, prename, name, function, cluster_param = None, step_type = None):
    


        if ( step_type is not None ):
            self._analysis[ name ][ step_type ] = 1
            print("Step type: {}-{} type:{}".format(prename, name, step_type))
            pp.pprint ( self._analysis )

        step = Step( pipeline = self,
                           name = name, 
                           function = function, 
                           step_type = step_type)

        self._analysis[ name ] = step



        if ( prename not in self._flow ):
            self._flow[ prename ] = []

        self._flow[ prename ].append( name )

        return step


    def next_step(self, prename, name, function, cluster_param=None):
        return self.add_step( prename, name, function, cluster_param);

    def global_merge_step(self, prename, name, function, cluster_param=None):
        self.add_step( prename, name, function, cluster_param, 'sync');


    def thread_merge_step(self, pre_name, name, function, cluster_param=None):
        self.add_step( prename, name, function, cluster_param, 'thread_sync');



    def print_flow(self, starts = None ):

        pp.pprint( self._flow )
        pp.pprint( self._analysis )
        exit()

        if ( starts is None ):
            starts  = self._start_steps

        if ( len( starts ) == 0):
            print( "No start steps defined")
            exit()



        analyses = []

        for start in starts:
            set_analyse_dependency( start )

        logic_names = starts

        print( "Starting with: {} \n".format( starts ))
        print( "--------------------------------------------------\n")

        while len( logic_names):
            current_logic_name = logic_names.pop()
            print( "{} queue: [{}]".format( current_logic_name, logic_names))
  
            self._analyses.push( current_logic_name )

            if current_logic_name not in self._analysis:
                print( "No information on {} in the analysis dict\n".format( current_logic_name ))
                exit()
            else:
                function = self._analysis[ current_logic_name ][ 'function' ]


            next_logic_names = next_analysis( current_logic_name )
      

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


