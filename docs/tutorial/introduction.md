<img src="fox.png" width="150" height="150">

### Geoffrey C. FOX


# Introduction to Big Data Problems and Systems


Big data problems can be classified into three main categories: batch processing (Hadoop), stream processing (Apache Flink and Apache Heron) and iterative machine learning and graph problems (Apache Spark). Each of these problems have different processing, communication and storage requirements. Therefore, each system provides separate solutions to these needs.

All these systems use dataflow programming model to perform distributed computations. With this model, big data frameworks represent a computation as a generic graph where nodes doing computations and the edges representing the communication. The nodes of the graph can be executed on different machines in the cluster depending on the requirements of the application.

We identify four key tasks in big data systems: 1) Acquiring computing resources, 2) Spawning and managing executor processes/threads, 3) Handling communication between processes, and 4) Managing the data including both static and intermediate. An independent component can be developed for each of these tasks. However, current systems provide tightly coupled solutions to these tasks excluding the resource scheduling.

Twister2 [1-3] is a loosely-coupled component-based approach to big data. Each of the four essential abstractions have different implementations to support various applications. Therefore, it has a pluggable architecture. It can be used to solve all three types of big data problems mentioned above.


<span style="color: green"> More content will be added soon.... </span>

### References

1. Supun Kamburugamuve, Kannan Govindarajan, Pulasthi Wickramasinghe, Vibhatha Abeykoon, Geoffrey Fox, "Twister2: Design of a Big Data Toolkit" in  EXAMPI 2017 workshop November 12 2017 at SC17  conference, Denver CO 2017.

2. Supun Kamburugamuve, Pulasthi Wickramasinghe, Kannan Govindarajan, Ahmet Uyar, Gurhan Gunduz, Vibhatha Abeykoon, Geoffrey Fox, "Twister:Net - Communication Library for Big Data Processing in HPC and Cloud Environments", Proceedings of Cloud 2018 Conference July 2-7 2018, San Francisco.

3. Kannan Govindarajan, Supun Kamburugamuve, Pulasthi Wickramasinghe, Vibhatha Abeykoon, Geoffrey Fox, "Task Scheduling in Big Data - Review, Research: Challenges, and Prospects", Proceedings of 9th International Conference on Advanced Computing (ICoAC), December 14-16, 2017, India.
