# Tutorial on Twister2 for 5th International Winter School on Big Data

Big data problems can be classified into three main categories: batch processing (Hadoop), stream processing (Apache Flink and Apache Heron) and iterative machine learning and graph problems (Apache Spark). Each of these problems have different processing, communication and storage requirements. Therefore, each system provides separate solutions to these needs.

All these systems use dataflow programming model to perform distributed computations. With this model, big data frameworks represent a computation as a generic graph where nodes doing computations and the edges representing the communication. The nodes of the graph can be executed on different machines in the cluster depending on the requirements of the application.

We identify four key tasks in big data systems

1. Job Submission (Acquiring compute resources and managing a job),
2. Parallel communication between processes,
2. Executing user logic with threads and processes
4. Managing the data including both static and intermediate.

An independent component can be developed for each of these tasks. However, current systems provide tightly coupled solutions to these tasks excluding the resource scheduling.

Twister2 [1-3] is a loosely-coupled component-based approach to big data. Each of the four essential abstractions have different implementations to support various applications. Therefore, it has a pluggable architecture. It can be used to solve all three types of big data problems mentioned above.

Twister2 provides a data analytics hosting environment where it supports different data analytics
including streaming, data pipelines and iterative computations. Unlike many other big data systems that are designed around user APIs, Twister2 is built from bottom
up to support different APIs and workloads. Our vision for Twister2 is a complete computing environment for data analytics.


In this tutorial, we review big data problems and systems, explain Twister2 architecture and features,
provide examples for developing and running applications on Twister2 system. By learning Twister2,
big data developers will have an experience with a flexible big data solution that can be used to
solve all three types of big data problems.

Twister2 is an ongoing open source project at Indiana University. It started in the 4th quarter of 2017.

* Github - https://github.com/DSC-SPIDAL/twister2
* Documentation - https://twister2.gitbook.io/twister2
* User List -  twister2@googlegroups.com
* Slack Channel - dsc-twister.slack.com

## What you will learn from this tutorial

By taking this tutorial you will;

 * Get an introductory information on big data systems and Apache big data solutions
 * Learn Twister2 concepts
 * Learn how job submission is done in Twister2 and related resource schedulers
 * Learn how the parallel communication happens Twister2
 * Learn the task system of Twister2
 * Learn how to develop big data solutions


## Syllabus

This Tutorial consists of several presentations and hands-on examples. We have prepared a docker image for you to easily 
run the examples in your laptop as well as a cluster deployment of Twister2.  

This tuorial website consists of the following sections.

1. [Apache data stack](big-data-stack.md)
2. [Twister2 overview](twister2-overview.md)
3. [Hands-on with Twister2](developing.md)
4. [Summary and future work](conclusion.md)

Here are the introductory slides we will go through in this Tutorial.

| Content | Description | Tutorial Section |
| ------------- | ------------- | ----------- |
| [Twister2 Presentation](https://docs.google.com/presentation/d/1FcoMfEd5g4cwR9K47PwIwvuioPPxdI9xgjVro2ipSTQ/edit?usp=sharing) | Introducing Twister2 | [Twister2 Overview](twister2-overview.md) |
| [Job Submission Presentation](https://docs.google.com/presentation/d/1Qs-eV9hTgyNRrDSvC5iCc2EmeUKQFtiVlTrTUZpsoAM/edit?usp=sharing)  | Explains Twister2 Job Submission and Resource abstractions  | [Twister2 Overview](twister2-overview.md#job-submission) |
| [Communication Presentation](https://docs.google.com/presentation/d/1-rSL3SIFp03YgU8hTJcGMNdSJev8gQwhyTgS3FbYaqk/edit?usp=sharing) | Explains the parallel communication model  | [Twister2 Overview](twister2-overview.md#communication) |
| [Task System Presentation](https://docs.google.com/presentation/d/1CpeBgKcM5NnIB0EdR0L5oWtfZdSG7kNlcEzyZPW8nuI/edit?usp=sharing) | Explains the task layer and API of Twister2 | [Twister2 Overview](twister2-overview.md#task-system) |

Here are the slides for Hands on section.

| Content | Section |
| ------------- | ------------- |
| [Hello World](https://docs.google.com/presentation/d/1ZMeO5aofZZNKwoR66N6b4hzSJqlGlbWgZLOq8Ie6vl0/edit?usp=sharing) | [Hello World](developing.md#hello-world-example) |
| [Streaming](https://docs.google.com/presentation/d/17uDBBlQxqzLx3m_inOM9svYvANCEwF2nN1KUYDoqInM/edit?usp=sharing)  | [Streaming Word Count](developing.md#streaming-wordcount-example) |
| [Batch](https://docs.google.com/presentation/d/1hpBcy_-m5AuVJJxPdhX_5hnIVB4vUkiB6My0STp-dLA/edit?usp=sharing) | [Batch Word Count](developing.md#batch-wordcount-example)  |
| [Machine Learning](https://docs.google.com/presentation/d/1-AZXo3KjPEk7E-k7_Z5lSKdPk_9R4D8w9PgNrijQeUU/edit?usp=sharing) | [K-Means](developing.md#machine-learning-example-kmeans) |
| [Communication API]()| Available Soon|


## References

1. Supun Kamburugamuve, Kannan Govindarajan, Pulasthi Wickramasinghe, Vibhatha Abeykoon, Geoffrey Fox, "Twister2: Design of a Big Data Toolkit" in  EXAMPI 2017 workshop November 12 2017 at SC17  conference, Denver CO 2017.

2. Supun Kamburugamuve, Pulasthi Wickramasinghe, Kannan Govindarajan, Ahmet Uyar, Gurhan Gunduz, Vibhatha Abeykoon, Geoffrey Fox, "Twister:Net - Communication Library for Big Data Processing in HPC and Cloud Environments", Proceedings of Cloud 2018 Conference July 2-7 2018, San Francisco.

3. Kannan Govindarajan, Supun Kamburugamuve, Pulasthi Wickramasinghe, Vibhatha Abeykoon, Geoffrey Fox, "Task Scheduling in Big Data - Review, Research: Challenges, and Prospects", Proceedings of 9th International Conference on Advanced Computing (ICoAC), December 14-16, 2017, India.
