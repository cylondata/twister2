---
id: developing_twister2
title: Developing a Twister2 Applications
sidebar_label: Developing Applications
---

This guide walks you through creating a simple application using Twister2.

## Pre-Requisites

Twister2 Applications are developed as Java Applications. We recommend using Java 1.8 or higher.

It is easier to use Maven to develop the application.

## Creating the Maven application

Lets create an empty Maven application and after that add the Twister2 dependencies.

```bash
mvn archetype:generate -DgroupId="twister2-hello" -DartifactId="hello" -DarchetypeArtifactId="maven-archetype-quickstart"  -DinteractiveMode=false
```

## Maven Dependency

Now lets add the Twister2 dependencies to the pom. Please add the correct version as required.

```xml
    <dependencies>
        <dependency>
            <groupId>edu.iu.dsc.tws</groupId>
            <artifactId>api-java</artifactId>
            <version>0.2.2</version>
        </dependency>
        <dependency>
            <groupId>edu.iu.dsc.tws</groupId>
            <artifactId>resource-scheduler-java</artifactId>
            <version>0.2.2</version>
        </dependency>
        <dependency>
            <groupId>edu.iu.dsc.tws</groupId>
            <artifactId>task-java</artifactId>
            <version>0.2.2</version>
        </dependency>
    </dependencies>
```

You would want to develop your application as a combined jar. So add the following to the project.

```xml

  <build>
      <plugins>
          <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-compiler-plugin</artifactId>
              <version>3.1</version>
              <configuration>
                  <source>1.8</source>
                  <target>1.8</target>
              </configuration>
          </plugin>
          <plugin>
              <artifactId>maven-assembly-plugin</artifactId>
              <configuration>
                  <descriptorRefs>
                      <descriptorRef>jar-with-dependencies</descriptorRef>
                  </descriptorRefs>
                  <archive>
                      <manifest>
                      </manifest>
                  </archive>
              </configuration>
              <executions>
                  <execution>
                      <id>make-assembly</id>
                      <phase>package</phase>
                      <goals>
                          <goal>single</goal>
                      </goals>
                  </execution>
              </executions>
          </plugin>
      </plugins>
  </build>

```

Now lets add code to the project.

```java

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a Hello World example of Twister2. This is the most basic functionality of Twister2,
 * where it spawns set of parallel workers.
 */
public class HelloWorld implements IWorker {

    private static final Logger LOG = Logger.getLogger(HelloWorld.class.getName());

    @Override
    public void execute(Config config, int workerID,
                        IWorkerController workerController,
                        IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
        // lets retrieve the configuration set in the job config
        String helloKeyValue = config.getStringValue("hello-key");

        // lets do a log to indicate we are running
        LOG.log(Level.INFO, String.format("Hello World from Worker %d; there are %d total workers "
                        + "and I got a message: %s", workerID,
                workerController.getNumberOfWorkers(), helloKeyValue));
    }

    public static void main(String[] args) {
        // lets take number of workers as an command line argument
        int numberOfWorkers = 4;
        if (args.length == 1) {
            numberOfWorkers = Integer.valueOf(args[0]);
        }

        // first load the configurations from command line and config files
        Config config = ResourceAllocator.loadConfig(new HashMap<>());

        // lets put a configuration here
        JobConfig jobConfig = new JobConfig();
        jobConfig.put("hello-key", "Twister2-Hello");

        Twister2Job twister2Job = Twister2Job.newBuilder()
                .setJobName("hello-world-job")
                .setWorkerClass(HelloWorld.class)
                .addComputeResource(2, 1024, numberOfWorkers)
                .setConfig(jobConfig)
                .build();
        // now submit the job
        Twister2Submitter.submitJob(twister2Job, config);
    }
}


```

## Testing on IDE

In order to test your project on IDE, you have to add the following dependency
to your pom.xml

```xml
<dependency>
    <groupId>edu.iu.dsc.tws</groupId>
    <artifactId>local-runner-java</artifactId>
    <version>0.2.2</version>
</dependency>
```

Now change your main method as follows,

```java
public static void main(String[] args) {

        LocalSubmitter submitter = LocalSubmitter.prepare("twister2/config/src/yaml/conf");

        // lets take number of workers as an command line argument
        int numberOfWorkers = 4;
        if (args.length == 1) {
            numberOfWorkers = Integer.valueOf(args[0]);
        }

        // first load the configurations from command line and config files
        Config config = ResourceAllocator.loadConfig(new HashMap<>());

        // lets put a configuration here
        JobConfig jobConfig = new JobConfig();
        jobConfig.put("hello-key", "Twister2-Hello");

        Twister2Job twister2Job = Twister2Job.newBuilder()
                .setJobName("hello-world-job")
                .setWorkerClass(HelloWorld.class)
                .addComputeResource(2, 1024, numberOfWorkers)
                .setConfig(jobConfig)
                .build();
        // now submit the job
        //Twister2Submitter.submitJob(twister2Job, config);
        submitter.submitJob(twister2Job, config);
    }
```

With below line, you should point to a valid config directory of twister2.

```java
LocalSubmitter submitter = LocalSubmitter.prepare("twister2/config/src/yaml/conf");
```

With following two lines, we will submit the job to an emulated cluster on your local machine.

```java
submitter.submitJob(twister2Job, config);
```

NOTE : Adjust the number of workers, based on the resources available in your local machine and your testing requirements. 

Now run or debug your program with IDE as you would normally do with any Java application.


## Running/Deploying on Cluster

Build your project to generate a jar file with following command.

```bash
mvn install
```

After this we can run this code from the Twister2 installation directory

```bash
./bin/twister2 submit standalone jar [PATH TO JAR FILE] HelloWorld 8

```


