#
# 
# 
##

from __future__ import print_function, unicode_literals

import pprint as pp

DEBUG = 0


class Job_status( object ):
    FINISHED    =    1
    FAILED      =    2
    RUNNING     =    3
    QUEUEING    =    4
    RESUBMITTED =    5
    SUBMITTED   =    6
    KILLED      =   99
    UNKNOWN     =  100


class Job(object):

    status   = Job_status.SUBMITTED
    active   = True
    command  = None

    output_file  = None
    limit        = None
    step_name    = None
    pre_task_ids = None
    delete_file  = None
    thread_id    = None
    cmd          = None

    def __init__(self,  cmd, step_name, limit=None, delete_file=None, thread_id=None):

        self.cmd = cmd
        self.step_name = step_name

        if ( limit is not None ):
            self.limit = limit

        if ( delete_file is not None ):
            self.delete_file = delete_file

        if ( thread_id is not None ):
            self.thread_id = thread_id


    def set_status( self, status=None ):
        if ( status is not None ):
            self.status = status


class Job_list( object ):

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



class Step ( object ):

    name      = None
    function  = None
    cparams   = None
    step_type = None
    pipeline  = None

    def __init__( self, pipeline, name, function, cparams=None, step_type=None):
        self.pipeline  = pipeline
        self.name      = name
        self.function  = function
        self.cparams   = cparams
        self.step_type = step_type


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

    def __repr__(self):
        return "{name}".format( name=self.name )

    def __str__(self):
        return "{name}".format( name=self.name )

    # Generic step adder, wrapped by the functions below it
    def add_step( self, function, name=None, cparams = None, step_type = None  ):  
        return self.pipeline.add_step( self, function, name, cparams , step_type )

    def next(self, function, name=None, cparams=None):
        return self.pipeline.add_step( self, function, name, cparams);

    def merge(self, function, name=None, cparams=None):
        return self.pipeline.add_step( self, function, name, cparams, step_type='sync');

    def thread_merge(self, function, name=None, cparams=None):
        return self.pipeline.add_step( self, function, name, cparams, step_type='thread_sync');



class Step_manager( object ):

    _start_steps = []
    _steps       = []
    _step_flow   = {}
    _step_index  = {}

    _step_dependencies = {}

    _analysis_order = {}

    def __repr__(self):

        res  = "\n\nTask manager dump::\n"
        res += "-----------------------\n\n"

        res += "Start steps:\n"
        res += pp.pformat(self._start_steps )+"\n"

        res += "Steps:\n"
        res += pp.pformat(self._steps )+"\n"


        res += "flow:\n"
        res += pp.pformat(self._step_flow )+"\n"

        res += "index:\n"
        res += pp.pformat(self._step_index )+"\n"

        res += "dependencies:\n"
        res += pp.pformat(self._step_dependencies )+"\n"

        res += "analysis-order:\n"
        res += pp.pformat(self._analysis_order )+"\n"

        return res

    # basic household functions
    def add(self, step ):
        self._steps.append( step )
        self._step_index[ step.name ] = len(self._steps) - 1

    def add_start_step(self, step ):
        self.add( step )
        self._start_steps.append( step )

    def link_steps(self, step1, step2 ):
        if ( step1 not in self._step_flow):
            self._step_flow[ step1 ] = []

        self._step_flow[ step1 ].append( step2 )

    def next_steps( self, step):

#        print ("Nxt step for: ", type(step), step )

        if type( step ) is 'str':
            step = self._step_index( step )

        if step not in self._step_flow:
            return None

#        pp.pprint( self._step_flow[ step ] )

        # Otherwise it will return a pointer to the list, and as I pop
        # from this list later on it ruins everything
        return self._step_flow[ step ][:]

    def step_by_name( self, name):
        if name not in self._step_index:
            return None

        return self._steps[ self._step_index[ name ]]


    def steps_by_name( self, names=[]):

        res = []

        for name in names:
            if name not in self._rev_step:
                print( "Unknown step name: {}".format( name ))
            else:
                res.append( _steps[ self._step_index[ name ]] )

        return res


    def find_analysis_order( self, steps ):

          
        self._analysis_order[ steps[ 0 ] ] = 1;


        while len(steps):
            step = steps.pop()

            next_steps = self.next_steps( step )
            
            if ( next_steps is None ):
                break


            for next_step in next_steps:
                if (next_step not in self._analysis_order or 
                    self._analysis_order[ next_step ] <= self._analysis_order[ step ] + 1):

                    self._analysis_order[ next_step ] = self._analysis_order[ step ] + 1 

            steps += self.next_steps( step )




    def print_flow(self, starts = None ):



        if starts is None:
            starts = self._start_steps
        else:
            start = self.steps_by_name( starts )

        for start in starts:
            self.calc_analysis_dependencies( start )

        self.find_analysis_order( starts )

        print( self )


        print("")
        print( "Starting with: {} ".format( starts ))
        print( "--------------------------------------------------\n")

        steps = starts
        
        while steps:
            step = steps.pop()
            next_steps = self.next_steps( step )

            print( "{} queue: {}".format( step.name, next_steps))
  
            if next_steps is not None:
                for next_step in next_steps:

                    if step.step_type is None:
                        print( "{} --> {}\n".format( step.name, next_step.name))
                    else:
                        print( "{} --> {} {}\n".format( step.name, next_step.name, step.step_type))


                steps += next_steps

        print( "--------------------------------------------------\n")


    def set_step_dependency(self, step, dependency):

        if step not in self._step_dependencies:
            self._step_dependencies[ step ] = []

        self._step_dependencies[ step ].append( dependency )

    def get_step_dependencies( self, step ):
        if step not in self._step_dependencies:
            return None

        return self._step_dependencies[ step ]


    def calc_analysis_dependencies(self,  start_step ):

        next_steps = self.next_steps( start_step )

        for next_step in next_steps:
            self.set_step_dependency(  next_step, start_step )

        while ( next_steps ):
            next_step = next_steps.pop()
            
            new_steps = self.next_steps( next_step );

            if ( new_steps is None or new_steps == []):
                continue

            next_steps += new_steps

            for new_step in new_steps:

                self.set_step_dependency( new_step,  next_step )

                for dependency in self.get_step_dependencies( next_step ):
                    self.set_step_dependency( new_step, dependency )



class Pipeline( object ):


    project_name = "CCBG" 
    queue_name   = ""
    project       = ""

    
    # For housekeeping to see how long the processign took
    _start_time = None
    _end_time   = None

    # when was the run information last saved
    _last_save      =   None
    # How often to save, in secs
    save_interval  = 300

    max_retry      =   3
    _failed_tasks   =   0 # failed jobs that cannot be restarted. 

    sleep_time     =   30
    max_sleep_time =  300
    _sleep_start   =  sleep_time
    sleep_increase =   30

    # to control that we do not flood the cluster, or if local block server machine.
    # -1 is no limit
    max_jobs       =  -1 

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

    _step_manager = Step_manager()

    _start_steps = []


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


    def _function_to_name(self, func ):

        if ( not callable( func )):
            print( "{}:: parameter is not a function, it is a {}".format( '_function_to_name', type( func )))
            exit()

        if ( func.__module__ is None or func.__module__ == "__main__"):
            name = "{}".format( func.__name__)
        else:
            name = "{}.{}".format( func.__module__, func.__name__)

        return name

    def start_step(self, function, name=None, cluster_params = None):

        # If no name was provided use the name of the function
        # instead. If the function comes from a module add that to the
        # name as well
        if name is None:
            name = self._function_to_name( function )


        start_step = Step( pipeline = self,
                           name = name, 
                           function = function)        
        
        self._step_manager.add_start_step( start_step )
        
        return start_step



    # Generic step adder, wrapped in the few functions below it
    def add_step( self, prev_step, function, name=None, cluster_param=None, step_type=None):


        # If no name was provided use the name of the function
        # instead. If the function comes from a module add that to the
        # name as well
        if name is None:
            name = self._function_to_name( function )

        print(" Prebv step type: {}".format( type(prev_step) ))


        if (callable(prev_step)):
            prev_step = self._function_to_name( prev_step  )

        if (isinstance(prev_step, basestring)):
            prev_step = self._step_manager.step_by_name( prev_step )


        print(" Prebv step type: {}".format( type(prev_step) ))

        step = Step( pipeline = self,
                     name = name, 
                     function = function, 
                     step_type = step_type)



        self._step_manager.add( step )
        self._step_manager.link_steps( prev_step, step )

        return step

    # Simple wrapper functions for the add_step function above.
    def next_step(self, prev_step, function, name=None, cluster_param=None):
        return self.add_step( prev_step, function, name, cluster_param);

    def global_merge_step(self, prev_step, function, name=None, cluster_param=None):
        self.add_step( prev_step, function, name, cluster_param, 'sync');

    def thread_merge_step(self, prev_step, function, name=None, cluster_param=None):
        self.add_step( prev_step, function, name, cluster_param, 'thread_sync');


    def print_flow(self):
        self._step_manager.print_flow()









class thread( object) :
    pass

class threads( object ):
    thread_counter   = 0 # This is for generating internal thread_id 

    def next_id():
        thread_counter += 1
        return thread_counter
    pass
