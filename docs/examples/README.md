# Examples

In the example section we discuss examples for both batch and streaming tasks considering 
two components in Twister2. Using Twister2:Net the communication based examples
are provided under the communication examples, including collective communication examples 
and complex communication graphs for multiple tasks. 

The task level examples provide an abstraction for the communication layer so that
the users doesn't have to create communication among tasks, the task layer does it
for the user defined task graph. In the task level examples, the user has to just create
the task graph and build the graph and internally all the communication is being handled
by the communication abstraction done in task layer. 


We have developed many examples that uses both Twister2 core functions as well as higher level abstractions.

Make sure to install Twister2 before running the examples. The installation insturctioncs can be found in [Installation Guide]()



* [Communication API](examples/collectives/comms/communication.md)
* [Task API](examples/collectives/task/task.md)
* [Task Examples](examples/collectives/task/task_examples.md)
* [Communication Examples](examples/collectives/comms/comms_examples.md)
* [K-Means](examples/kmeans/kmeans_example.md)