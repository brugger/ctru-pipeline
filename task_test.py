#!/usr/bin/python
# 
# 
# 
# 
# Kim Brugger (06 Jun 2017), contact: kim@brugger.dk

import sys
import pprint
pp = pprint.PrettyPrinter(indent=4)

#import  ccbg_pipeline.task

from ccbg_pipeline import *

print task_status.SUBMITTED

P = Pipeline()
P.max_retry( 5 )
print ( P.max_retry(  ) )


step_a = P.start_step("b", 'fb')
step_a = P.start_step("a", 'fa').next("b", 'fbc').next("c", "fcf")

def test():
    pass


func = test

print func.__name__

P.print_flow()
