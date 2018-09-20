Twister2 Logging
================

Logging in distributed environments is an important task. 
Cluster resource schedulers provide logging mechanisms. For example, 
Kubernetes saves applications logs in files under /var/log directory in agent machines. 
These log files are shown to users with a web based application. 
It is called Dashboard. Users can download the logs of their applications 
to their local machines.

Since there are many agent machines in a Kubernetes cluster, 
these files are distributed in the cluster. 
They are difficult to access after the application completed. 
In addition, these log files are deleted when pods are deleted. 
Therefore, we implemented a persistent logging mechanism for Twister2 jobs. 

We save all log files in a job to a persistent volume. 
All log files in a job is saved under the same directory. The logging directory is named as:

```bash
    logs
```    

A log file is created for each worker in this log directory. 
Worker log files are named as:

```bash
  * worker-0.log.0
  * worker-1.log.0
  * worker-2.log.0
  * ...
```

There may be many log files for each worker. 
In that case, more log files are created with increasing suffix value.

## Implementation
We use Java logger redirection to a persistent file.
we add a FileHandler to the root logger of Java. The file is located in the persistent volume. 
So, Java logger directs the log messages to both the console and to this persistent log file. 

We implemented the logger in the class:

```bash
  edu.iu.dsc.tws.common.logging.LoggingHelper
```  

## Configuration Parameters
Five configuration parameters are added for persistent logging. 
First one shows whether the user wants Twister2 to save log messages to the persistent storage. 
When its value is “true”, persistent logging is enabled. Otherwise, persistent value is disabled. 
Configuration parameter name is called: 

```bash
  persistent.logging.requested
```

Other configuration parameters: 

```bash
  twister2.logging.level
  twister2.logging.max.file.size.mb
  twister2.logging.maximum.files
  twister2.logging.redirect.sysouterr
```    

The second parameters determines the log level. By default, it is INFO. 
Valid logging levels: FINEST, FINER, FINE, CONFIG, INFO, WARNING, SEVERE

The third parameter shows the maximum size of the log files. 
When a log file size reaches to this value, it is moved to another file.  
Its default value is 100 MB. 

The fourth parameter shows the maximum number of log files for a worker. 
Its default value is 5. If there are more logs files for a worker. 
The oldest ones will be deleted. 

The last parameter shows whether System.out and System.err streams should also be redirected 
to persistent storage. It is a boolean value. By default, its value is false.
If this is enabled, all regular system output and error messages will be directed to the log files. 
Users will not be able to see log messages on the screen. 

## Sample Usage
Kubernetes Twister2 workers initialize the worker loggers as the following. 
This method needs to be called as early as possible when the workers start. 

```bash
  public static void initWorkerLogger(int workerID, K8sPersistentVolume pv, Config cnfg) {

    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cnfg));

    // if persistent logging is requested, initialize it
    if (pv != null && LoggingContext.persistentLoggingRequested(cnfg)) {

      if (LoggingContext.redirectSysOutErr(cnfg)) {
        LOG.warning("Redirecting System.out and System.err to the log file. "
          + "Check the log file for the upcoming log messages. ");
      }

      String logFile = K8sPersistentVolume.WORKER_LOG_FILE_NAME_PREFIX + workerID;
      LoggingHelper.setupLogging(cnfg, pv.getLogDirPath(), logFile);

      LOG.info("Persistent logging to file initialized.");
    }
  }
```  

