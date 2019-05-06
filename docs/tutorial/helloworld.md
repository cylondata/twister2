# Hello World Example

In this part we will explain how the HelloWorld example runs.
You will get an idea how to write a simple twister2 job,
submit it and then check the output.

In order to run these examples, you should have a running Kubernetes or a Mesos cluster, configured as stated on Twister2 website.  
You can submit jobs to chosen resource scheduler by using Twister2 executable;
* ./bin/twister2

When submitting jobs to Kubernetes clusters, you need to specify the cluster name as "kubernetes".  Likewise this parameter should be “mesos” if submitting to a Mesos cluster. 

You can submit HelloWorld job in examples package with 8 workers as;
* ./bin/twister2 submit kubernetes jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8

If you are using our testbed cluster “Echo”, 
* login to your account 
* change you directory to  twister2/twister2/bazel-bin/scripts/package/twister2-0.2.1/ 
* then run the command above. 

You can check the status of the submitted job through the dashboard provided by the resource scheduler.  For our Echo Cluster the addresses are;
* Kubernetes ---> http://149.165.150.81:8080/#/jobs
* Mesos---> http://149.165.150.81:5050

HelloWorld job runs for 1 minute. After 1 minute, the job becomes COMPLETED if everything is fine.

Lets check the code; In the main method;
* First set the number of workers by using the arguments passed by the command line. If there is no arguments then the default value is 4
* Then we load the configurations from command line and config files(yaml files) and put it in a JobConfig object. 
  
 ` Config config = ResourceAllocator.loadConfig(new HashMap<>());
  JobConfig jobConfig = new JobConfig();
  jobConfig.put("hello-key", "Twister2-Hello");`

We then create the Twister2 job object by the following code. 
   
   		Twister2Job twister2Job = Twister2Job.newBuilder()
   		.setJobName("hello-world-job")
   		.setWorkerClass(HelloWorld.class)
   		.addComputeResource(2, 1024, numberOfWorkers)
   		.setConfig(jobConfig)
   		.build();

Last thing we do is submitting the job
   
   * `Twister2Submitter.submitJob(twister2Job, config);`

After the submission execute method of this HelloWorld class will be called on each worker. So that means the code in the execute method will be run on every worker
    `   public void execute(Config config, int workerID,    
        IWorkerController workerController,  
        IPersistentVolume persistentVolume, 
         IVolatileVolume volatileVolume)`

In our case the rest of the code writes hello message from each worker including their IDs, total number of workers and the message and then sleeps for one minute

    // lets retrieve the configuration set in the job config
    String helloKeyValue = config.getStringValue("hello-key");

    // lets do a log to indicate we are running
    LOG.log(Level.INFO, String.format("Hello World from Worker %d; there are %d total workers "
            + "and I got a message: %s", workerID,
        workerController.getNumberOfWorkers(), helloKeyValue));

    List<JobMasterAPI.WorkerInfo> workerList = null;
    try {
      workerList = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }
    String workersStr = WorkerInfoUtils.workerListAsString(workerList);
    LOG.info("All workers have joined the job. Worker list: \n" + workersStr);

    try {
      LOG.info("I am sleeping for 1 minute and then exiting.");
      Thread.sleep(60 * 1000);
      LOG.info("I am done sleeping. Exiting.");
    } catch (InterruptedException e) {
      LOG.severe("Thread sleep interrupted.");
    }


You can access to the presentation using the link below

[Hello Word example](https://docs.google.com/presentation/d/1ZMeO5aofZZNKwoR66N6b4hzSJqlGlbWgZLOq8Ie6vl0/edit#slide=id.p)


