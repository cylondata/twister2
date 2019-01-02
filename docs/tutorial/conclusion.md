<span style="display:block;text-align:left">![Geoffrey C. FOX](fox.png)</span>

# Summary and Future Work

In this tutorial, we have reviewed big data problems and systems,
have explained Twister2 architecture and features,
have provided examples for developing and running applications
on Twister2 system.

Twister2 is an ongoing project. We are planning to add the following features in near future.

* Naiad model based Task system for Machine Learning
* Native MPI integration to Mesos, Yarn
* Dynamic task migrations
* RDMA and other communication enhancements
* Integrate parts of Twister2 components as big data systems enhancements (i.e. run current Big Data software invoking Twister2 components)
    * Heron (easiest), Spark, Flink, Hadoop (like Harp today)
    * Tsets become compatible with RDD (Spark) and Streamlet (Heron)
* Support different APIs (i.e. run Twister2 looking like current Big Data Software),Hadoop, Spark (Flink), Storm
* Refinements like Marathon with Mesos etc.
* Function as a Service and Serverless
* Support higher level abstractions
    * Twister:SQL (major Spark use case)
* Graph API

