# Benchmarking Twister2

Twister2 has a basic benchmark suite to test and verify communication and task layers. Benchmark suite can be configured to automatically run twister2 examples with all possible combinations of a predefined set of parameters, which will be useful to perform performance tests. At the end of each iteration, it will generate a CSV report including all the observations including, configuration parameters, durations, output verification status etc.

The benchmark suite is located at ```util/test``` folder and you can define your own benchmarks inside the ```util/test/tests``` directory.

## Running benchmarks

You can start benchmark suite as follows.

```python launcher.py```

This will analyze ```util/test/tests``` for predefined benchmarks (.json files) and spawn twister2 standalone mode with different parameter configurations to record timing etc.

## Running a selected set of benchmarks

Instead of running everything, you may also run a selected set of benchmarks as follows

```python launcher.py benchmark_id1,benchmark_id2```

This will execute only the definitions having id ```benchmark_id1 ``` and ```benchmark_id2```

## Configuring Benchmarks

### base.json

This file holds the main configuration parameters and entry points.

```json
{
  "jarRootDir": "Root Directory to search for JAR containing the twister2 job ",
  "t2Bin": "pointer to twister2 bin/twister2 executable",
  "jdk": "pointer to jdk or jvm"
}
``` 

### Defining Benchmarks

The format of a benchmark definition file is as follows.

```json
{
  "id": "communication_base",
  "directory": {
    "relativeToRoot": true,
    "path": ""
  },
  "jar": "libexamples-java.jar",
  "className": "edu.iu.dsc.tws.examples.comms.ExampleMain",
  "args": [
    {
      "id": "itr",
      "name": "Iterations",
      "optional": false,
      "values": {
        "type": "fixed",
        "value": 500
      }
    },
    {
      "id": "workers",
      "name": "Workers",
      "optional": false,
      "values": {
        "type": "fixed",
        "value": 4
      }
    },
    {
      "id": "size",
      "name": "Data Size",
      "optional": false,
      "values": {
        "type": "steps",
        "from": 128,
        "to": 8192,
        "step": 128
      }
    },
    {
      "id": "warmupitr",
      "name": "Warmup Iterations",
      "optional": false,
      "values": {
        "type": "fixed",
        "value": 10
      }
    },
    {
      "id": "stream",
      "name": "Stream",
      "optional": false,
      "values": {
        "type": "none"
      }
    }
  ]
}
```

| Attribute | Description |
|---|---|
| id | Each definition file should have a globally unique ID|
|directory|Directory for the JAR file. If ```relativeToRoot``` is set to ```true```, the location will be considered relative to the ```jarRootDir``` from base.json|
|jar|Name of the jar file|
|className|Name of the main class|
|args|Set of arguments to pass into the main method of the main class |

#### Format of Args

Arguments can be defined as a json object.

```json
{
  "id": "itr",
  "name": "Iterations",
  "optional": false,
  "values": {
    "type": "fixed",
    "value": 500
   }
}
```

| Attribute | Description |
|---|---|
|id|Each argument should has a unique id relative the current benchmark definition. This will be used as an argument flag when invoking jar file. For example, above abutment will be passed as  ```java -jar jarfile.jar -itr 500```|
|name|This name will be used as the column of the final CSV file|
|optional|Indicate whether this argument is optional or not. If an argument is optional, suite will run a set of iterations with all possible values for this argument, and also it will run another set of iterations without this argument|
|values|Definition of possible values for this argument|

#### Format of values object

Possible values for an argument can be defined as json object as follows.

```json
{
  "type": "fixed",
  "value": 500
}
```

##### Types of values

Currently twister2 benchmark suite supports following types of argument values definitions.

1. fixed

  In fixed type, this argument will be always passed as a constant to the jar.

2. array

```json
{
      "id": "stages",
      "name": "Stages",
      "optional": false,
      "values": {
        "type": "array",
        "array": [
          "1,8"
        ]
      }
    }
```

In array type, you may define a set of possible values as shown in above example.

3. Steps

```json
{
      "id": "size",
      "name": "Data Size",
      "optional": false,
      "values": {
        "type": "steps",
        "from": 0,
        "to": 15,
        "step": 5
      }
    }
```

In steps type, benchmark suite will generate a set of possible values based on the ```from,to and step``` defined. For example, for above configuration, the possible values will be ```0,5,10,15```.

4. None

```json
{
      "id": "verify",
      "name": "Verify",
      "optional": false,
      "values": {
        "type": "none"
      }
    }
```

In none type, argument will be specified without any value. For example, above argument will be passed as follows.

```bash
java -jar jarname.jar -verify -anotherFlag 1200
```

### Defining child benchmarks

You can define a child benchmark as follows to inherit all the properties from it's parent.

```json
{
  "id": "one_to_many_base",
  "parent": "communication_base",
  "args": [
    {
      "id": "stages",
      "name": "Stages",
      "optional": false,
      "values": {
        "type": "array",
        "array": [
          "1,8"
        ]
      }
    }
  ]
}
```

````json
{
  "id": "comms_bcast",
  "parent": "one_to_many_base",
  "resultsFile": "comms_bcast.csv",
  "args": [
    {
      "id": "op",
      "name": "Operation",
      "optional": false,
      "values": {
        "type": "fixed",
        "value": "\"bcast\""
      }
    }
  ]
}
````

In above example, ```one_to_many_base``` inherits everything from it's parent and define an additional argument ```stages```. Similarly ```comms_bcast``` inherits all the properties from both ```one_to_many_base```  and ```communication_base```. 

**If benchmark suite detects a json configuration as a parent for another definition, it won't run it. Only the root or child definitions will be executed.**

### Benchmark Report

Benchmark suits generates a CSV report file including a summary of results as follows

```csv
Verified,Workers,Data Size,Stages,Warmup Iterations,Stream,Operation,Total Time (ns),Iterations,Average Time (ns),
Not Performed,128,128,1/8,10000,true,bcast,4.5086668E8,50000,53795.60888,
Not Performed,128,128,1/8,10000,true,bcast,2.81520047E8,50000,33020.64082,
Not Performed,128,256,1/8,10000,true,bcast,3.68366037E8,50000,50965.56262,
Not Performed,128,128,1/8,10000,true,bcast,4.99461148E8,50000,71344.79224,
Not Performed,128,128,1/8,10000,true,bcast,2.59625461E8,50000,37043.11802,
Not Performed,128,128,1/8,10000,true,bcast,3.98053658E8,50000,50565.40946,
Not Performed,128,256,1/8,10000,true,bcast,4.30750903E8,50000,46554.4231,
Not Performed,128,384,1/8,10000,true,bcast,4.36714506E8,50000,57960.8657,
Not Performed,128,128,1/128,10000,true,bcast,1.6876045679E10,50000,1269817.83494,
Not Performed,128,128,1/128,10000,true,bcast,2.3365182376E10,50000,1814343.599,
Not Performed,128,256,1/128,10000,true,bcast,1.1952036204E10,50000,970323.24592,
Not Performed,128,384,1/128,10000,true,bcast,1.9385305632E10,50000,1499072.71986,
Not Performed,128,512,1/128,10000,true,bcast,3.7422337696E10,50000,2900719.07566,
Not Performed,128,640,1/128,10000,true,bcast,2.9975160264E10,50000,2347069.67854,
Not Performed,128,768,1/128,10000,true,bcast,1.8697905574E10,50000,1455182.97132,
Not Performed,128,896,1/128,10000,true,bcast,2.2884656927E10,50000,1800624.62124,
Not Performed,128,1024,1/128,10000,true,bcast,1.73158645221E11,50000,1.336154577186E7,
Not Performed,128,1152,1/128,10000,true,bcast,1.89358183229E11,50000,1.449971312012E7,
Not Performed,128,1280,1/128,10000,true,bcast,1.73612895419E11,50000,1.33740494506E7,
Not Performed,128,1408,1/128,10000,true,bcast,1.83786012116E11,50000,1.39499546007E7,
Not Performed,128,1536,1/128,10000,true,bcast,1.76920346997E11,50000,1.353325271198E7,
Not Performed,128,1664,1/128,10000,true,bcast,1.69401352936E11,50000,1.293049998288E7,
Not Performed,128,1792,1/128,10000,true,bcast,1.74388799268E11,50000,1.341673140952E7,
Not Performed,128,1920,1/128,10000,true,bcast,1.76224134556E11,50000,1.347409234638E7,
Not Performed,128,2048,1/128,10000,true,bcast,1.75166737863E11,50000,1.339366238642E7,
Not Performed,128,2176,1/128,10000,true,bcast,1.7915152928E11,50000,1.38268006356E7,
Not Performed,128,2304,1/128,10000,true,bcast,1.77446562244E11,50000,1.350657902412E7,
Not Performed,128,2432,1/128,10000,true,bcast,1.85446124998E11,50000,1.409667511456E7,
Not Performed,128,2560,1/128,10000,true,bcast,1.69902418899E11,50000,1.307200768156E7,
Not Performed,128,2688,1/128,10000,true,bcast,1.71713292617E11,50000,1.32171850037E7,
Not Performed,128,2816,1/128,10000,true,bcast,1.82236393547E11,50000,1.394721483284E7,
Not Performed,128,2944,1/128,10000,true,bcast,1.73751822497E11,50000,1.333692961452E7,
Not Performed,128,3072,1/128,10000,true,bcast,1.72964932955E11,50000,1.328497429018E7,
Not Performed,128,3200,1/128,10000,true,bcast,1.71224095728E11,50000,1.3114826647E7,
Not Performed,128,3328,1/128,10000,true,bcast,1.70843410688E11,50000,1.310339248622E7,
Not Performed,128,3456,1/128,10000,true,bcast,1.7150750965E11,50000,1.314770285628E7,
Not Performed,128,3584,1/128,10000,true,bcast,1.74626979591E11,50000,1.336732442114E7,
Not Performed,128,3712,1/128,10000,true,bcast,1.80190296977E11,50000,1.375548090648E7,
Not Performed,128,3840,1/128,10000,true,bcast,1.79574792176E11,50000,1.383994504508E7,
Not Performed,128,3968,1/128,10000,true,bcast,1.79097113554E11,50000,1.367853750884E7,
Not Performed,128,4096,1/128,10000,true,bcast,1.74005847105E11,50000,1.34263681024E7,
Not Performed,128,4224,1/128,10000,true,bcast,1.69580633743E11,50000,1.300204078868E7,
Not Performed,128,4352,1/128,10000,true,bcast,1.76287896741E11,50000,1.354152601874E7,
Not Performed,128,4480,1/128,10000,true,bcast,1.77422663273E11,50000,1.359743743042E7,
Not Performed,128,4608,1/128,10000,true,bcast,1.81560221177E11,50000,1.374463961932E7,
Not Performed,128,4736,1/128,10000,true,bcast,1.70653419718E11,50000,1.321157439444E7,
Not Performed,128,4864,1/128,10000,true,bcast,1.90143780921E11,50000,1.456616376296E7,
Not Performed,128,4992,1/128,10000,true,bcast,1.79797283787E11,50000,1.376495798572E7,
Not Performed,128,5120,1/128,10000,true,bcast,1.82882980189E11,50000,1.403814442386E7,
Not Performed,128,5248,1/128,10000,true,bcast,1.71194273169E11,50000,1.31500986017E7,
Not Performed,128,5376,1/128,10000,true,bcast,1.85176077644E11,50000,1.416959453758E7,
Not Performed,128,5504,1/128,10000,true,bcast,1.75630636979E11,50000,1.344816410238E7,
Not Performed,128,5632,1/128,10000,true,bcast,1.7688980708E11,50000,1.361641127542E7,
Not Performed,128,5760,1/128,10000,true,bcast,1.85891226524E11,50000,1.41107273382E7,
Not Performed,128,5888,1/128,10000,true,bcast,1.66559523798E11,50000,1.290325805284E7,
Not Performed,128,6016,1/128,10000,true,bcast,1.696966135E11,50000,1.310402980322E7,
Not Performed,128,6144,1/128,10000,true,bcast,1.71307400657E11,50000,1.319417248508E7,
Not Performed,128,6272,1/128,10000,true,bcast,1.81544589866E11,50000,1.394469652278E7,
Not Performed,128,6400,1/128,10000,true,bcast,1.73488839245E11,50000,1.332507358708E7,
Not Performed,128,6528,1/128,10000,true,bcast,1.7253211491E11,50000,1.331324542574E7,
Not Performed,128,6656,1/128,10000,true,bcast,1.71052421773E11,50000,1.321165804286E7,
Not Performed,128,6784,1/128,10000,true,bcast,1.79829879959E11,50000,1.373182876798E7,
Not Performed,128,6912,1/128,10000,true,bcast,1.77407712583E11,50000,1.367942826112E7,
Not Performed,128,7040,1/128,10000,true,bcast,1.80916317835E11,50000,1.387490513422E7,
Not Performed,128,7168,1/128,10000,true,bcast,1.71001136418E11,50000,1.315990850176E7,
Not Performed,128,7296,1/128,10000,true,bcast,1.71847101521E11,50000,1.30961725418E7,
Not Performed,128,7424,1/128,10000,true,bcast,1.70631525264E11,50000,1.316232779922E7,
Not Performed,128,7552,1/128,10000,true,bcast,1.7076477131E11,50000,1.311049186548E7,
Not Performed,128,7680,1/128,10000,true,bcast,1.71493613448E11,50000,1.325231329484E7,
Not Performed,128,7808,1/128,10000,true,bcast,1.8272220511E11,50000,1.402612898148E7,
Not Performed,128,7936,1/128,10000,true,bcast,1.67156797644E11,50000,1.292076095258E7,
Not Performed,128,8064,1/128,10000,true,bcast,1.81723512055E11,50000,1.395780516188E7,
Not Performed,128,8192,1/128,10000,true,bcast,1.76410927699E11,50000,1.365974788318E7,
```

## Timing your benchmarks

If you want to write your own benchmark, you can use twister's timing utilities as follows.

Before recording anything, initialize the timers with the proffered unit.

```java
Timing.setDefaultTimingUnit(TimingUnit.NANO_SECONDS);
```

You can either use nano seconds or mili seconds to track running durations.

To mark the time at a certain point, call ```Timing.mark()``` as follows.

```java
Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, timingCondition);
```

```timingCondition``` is a boolean shortcut to avoid unnecessary if else statements. For example, if you only need to record timing in worker 0, instead of doing,

```java
if(workerId ==0){
  //mark timing
}
```

you may simply pass that to the timing condition.

```java
Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, workerId==0);
```

### Taking the differences and averages of times

```java
for(int i=0;i<10;i++){
  Timing.mark("start", timingCondition);
  //time consuming operation
  Timing.mark("end", timingCondition);
}
```

Since both ```start``` and ```end``` flags will be recorded for 10 times, you can take the average runtime for a single iteration as follows.

```java
Timing.averageDiff("start", "end", timingCondition)
```

If you need the values of each individual iteration, you may use ```diffs()``` method which returns a List of differences.

```java
Timing.diffs("start", "end", timingCondition)
```

## Recording the results of benchmarks

To record and write the results of your benchmark, you may use ```edu.iu.dsc.tws.examples.utils.bench.BenchmarkResultsRecorder```. For more usage examples, refer twister2 communication and tasks examples.

