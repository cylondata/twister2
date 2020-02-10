#  // Licensed under the Apache License, Version 2.0 (the "License");
#  // you may not use this file except in compliance with the License.
#  // You may obtain a copy of the License at
#  //
#  // http://www.apache.org/licenses/LICENSE-2.0
#  //
#  // Unless required by applicable law or agreed to in writing, software
#  // distributed under the License is distributed on an "AS IS" BASIS,
#  // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  // See the License for the specific language governing permissions and
#  // limitations under the License.

import numpy as np
from twister2.Twister2Environment import Twister2Environment
from twister2deepnet.deepnet.distributed.ProcessManager import ProcessManager



env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 2}])

# Your worker code starts here
print("Hello from worker {}".format(env.worker_id))

script_path = "/home/vibhatha/github/forks/twister2/deeplearning/pytorch/src/main/resources/bash/pytorchrunner"

pm = ProcessManager(env=env)
child = pm.spawn(spawnargv=[script_path], maxprocs=2, errorcode=[0,0])

int_data= env.gen_java_int_array(size=4)
for i in range(4):
    int_data[i] = i * 10
if env.worker_id == 0:
    child.send(int_data, 4, env.get_mpi_datatype_int(), 1, 0)

#comm.allReduce(localSum, globalSum, 1, env.get_mpi_datatype_double(), env.get_mpi_op_sum())

#print(globalSum)