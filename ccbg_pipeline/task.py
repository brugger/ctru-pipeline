#
# 
# 
##

from __future__ import print_function, unicode_literals

import pprint as pp

DEBUG = 0


def _function_to_name( func ):

    if ( not callable( func )):
        print( "{}:: parameter is not a function, it is a {}".format( '_function_to_name', type( func )))
        exit()

    if ( func.__module__ is None or func.__module__ == "__main__"):
        name = "{}".format( func.__name__)
    else:
        name = "{}.{}".format( func.__module__, func.__name__)

    return name


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



    def next_id():
	'''
	
        Gets the next job id from the class

	Args:

	:Returns:

	'''
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


    def steps_by_name( self, names=None):

        res = []

        if names is None:
            return res

        for name in names:

            if( callable( name )):
                name = _function_to_name( name )
                print( "Name is: {}".format( name ))
                
            if (isinstance(name, basestring) and name not in self._step_index):
                print( "Unknown step name: {}".format( name ))               
                exit()
            else:
                print( "Name is: {}".format( name ))
                print (self._steps[ self._step_index[ name ]])
                res.append( self._steps[ self._step_index[ name ]] )

        return res


    def find_analysis_order( self, steps ):

        steps = steps[:]
          
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



    def waiting_for_analysis(self, step, steps_done):

        dependencies = self.get_step_dependencies( step )
             
        if dependencies is None:
            return 0

        pp.pprint( dependencies )

        done = {}
        for step_done in steps_done:
            done[ step_done ] = 1

        for dependency in dependencies:
            if dependency not in done:
                print("{} is waiting for {}".format(step, dependency));
                return 1

        return 0



    def print_flow(self, starts = None ):

        pp.pprint(self)

        pp.pprint( starts )

        if starts is None:
            starts = self._start_steps
        else:
            starts = self.steps_by_name( starts )

            
        pp.pprint( starts )

        for start in starts:
            self.calc_analysis_dependencies( start )

        self.find_analysis_order( starts )

        print( self )


        print("")
        print( "Starting with: {} ".format( starts ))
        print( "--------------------------------------------------\n")

        steps = starts[:]
        
        steps_done = []
        
        while steps:
            step = steps.pop()
            next_steps = self.next_steps( step )

            steps_done.append( step )


            print( "{} queue: {}".format( step.name, next_steps))
  
            if next_steps is not None:
                for next_step in next_steps:

                    if step.step_type is None:
                        print( "{} --> {}\n".format( step.name, next_step.name))
                    else:
                        print( "{} --> {} {}\n".format( step.name, next_step.name, step.step_type))


                    if ( self.waiting_for_analysis(next_step, steps_done)):
                        pass
                    else:
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



    def start_step(self, function, name=None, cluster_params = None):

        # If no name was provided use the name of the function
        # instead. If the function comes from a module add that to the
        # name as well
        if name is None:
            name = _function_to_name( function )


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
            name = _function_to_name( function )

        print(" Prebv step type: {}".format( type(prev_step) ))


        if (callable(prev_step)):
            prev_step = _function_to_name( prev_step  )

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


    def print_flow(self, starts=None):
        self._step_manager.print_flow( starts )







    def run(self, starts=None):

        
        if starts is None:
            starts = self._start_steps
        else:
            starts = self.steps_by_name( starts )

            
        self._task_manager.validate_flow( starts )


        for start in starts:
            self._task_manager.calc_analysis_dependencies( start )

        self.find_analysis_order( starts )


        while ( True ):


            (started, queued, running ) = (0,0,0)

            active_jobs = self._job_manager.fetch_active_jobs();
            if ( len(active_jobs) == 0 and  not self._restarted_run ):

                for start in starts:
                    self._run_analysis( start );
                    queued += 1
                continue
            
            
            for active_job in active_jobs:
                # The job is no longer being tracked either due to crashing or finishing.
                if ( not active_job.tracking ):
                    continue
                

                step_name = active_job.step
                tread_id  = active_job.thread_id

                if ( active_job.status = Job_status.FINISHED ):
                    active_job.tracking = 0
                    next_steps = self._step_manager.next( step_name )

                    if ( next_steps is None or len(next_steps) == 0):
                        continue

                    for next_step in next_steps:
                        if ( next_step.sync == 'sync' or next_step.sync == 't_sync'):
                            if ( next_step.sync == 'sync'):
                            active_thread_id = 0

                            if ( self._thread_manager.no_restart( active_thread )):
                                continue

                            if ( retained_jobs > 0 ):
                                continue

                            if ( self._task_manager.depends_on_active_jobs( next_step )):
                                 continue

                             depends_on = []
                             for step in self._task_manager.flow.keys():
                                 for analysis in self._task_manager( flow( step )):
                                     depends_on.append( analysis )
                                

                            depends_jobs = fetch_jobs( depends_on )
                            all_treads_done = 0
                            for job ( depnds_jobs ):
                                if ( job.status != Job_status.FINISHED ):
                                    all_threads_done = 0
                                    break
                                
                                     # active_thread_id aware...

                            if ( all_threads_done ):

                                inputs  = []
                                job_ids = []
                                
                                for job in depnds_jobs:
                                    job.tracking = 0
                                    inputs.append( job.output )
                                    job_ids.append( jobs )
		
		

                            self.run_analysis( next_step, job_ids, inputs);
                            started += 1
                            
                        else:
                            run_analysis( $next_step, job, job.output)
                            started += 1
                elif (job.status == Job_status.FAILED or job.status == Job_status.KILLED):
                    job.tracking = 0
                elif ( job.status = Job_status.RUNNING):
                    queued += 1
                    running += 1
                else:
                    queued += 1

                    

    while ( self.max_jobs > 0 and self._job_submitted < self.max_jobs && len( self.retained_jobs )):

        params = retained_jobs.pop()
        started += 1


    check_n_store_state()
    print report()
    
#    system('clear');
#    print report_spinner();
#    report2tracker() if ($database_tracking);

    if ( len( queued ) == 0 and started == 0 and len( retained_jobs ) == 0):
        last



    last if ( ! $queued && ! $started && !@retained_jobs);

    if ( running == 0 and self.sleep_time < self.max_sleep_time):
        self.sleep_time += self.sleep_increase

    if ( running != 0 ):
        self.sleep_time = self._sleep_start
    


        
    sleep ( $sleep_time )
    check_jobs()

  print report()
#  report2tracker() if ($database_tracking);
  print total_runtime()
  print real_runtime()

  if ( no_restart ) {
    print("The pipeline was unsucessful with $no_restart job(s) not being able to finish\n");
  }
  

  if ( len(retained_jobs) > 0):
      print("Retaineded jobs: ". @retained_jobs . " (should be 0)\n") if ( @retained_jobs != 0);
#  $end_time = Time::HiRes::gettimeofday();
#  self.store_state();

  # $logger->debug( { 'type'     => "pipeline_stats",
  # 		   'program'  => $0,
  # 		   'pid'      => $$,
  # 		   'status'   => "FINISHED",
  # 		   'runtime'  => $end_time - $start_time });


  return no_restart



        

class thread( object) :
    pass

class threads( object ):
    thread_counter   = 0 # This is for generating internal thread_id 

    def next_id():
        thread_counter += 1
        return thread_counter
    pass
