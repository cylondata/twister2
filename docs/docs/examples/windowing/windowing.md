---
id: windowing
title: Windowing
sidebar_label: Windowing
---

Twister2 0.2.1 release includes an experimental windowing functionality. We are still working on 
bringing this to a complete API with the state of the art windowing support. 

In the current release, we have worked on implementing a windowing mechanism similar to Apache
Storm streaming engine. As same as Storm, we also have the windowing operation at the lower part of
the task graph. That is called Sink in Twister2 which is equivalent to Bolt in Storm. 

## Current Support

1. Tumbling Count Window
2. Tumbling Duration Window

We support this functionality in a dynamic way. User can create their own policies to handle the 
windowing logic. 

We provide, 

1. Eviction Policy (ex. CountEvictionPolicy or DurationEvictionPolicy)
2. Window Policy (ex. CountWindowPolicy or DurationWindowPolicy)

Eviction policy is the way to include the set of messages which need to be in the window. 
The window policy shows when it has to be triggered and sent to the user. We have adopted a similar
model like Storm to support these functionalities. 

Window Length == Sliding Length => Tumbling Window
WIndow Length > Sliding Length => Sliding Window
Window Length < Sliding Length => Invalid Window

Currently we have two experimental examples. 

### Tumbling Windowing

Tumbling means we have a fixed window size for the window, where the sliding interval equals to the 
size of the window. 

#### Inbuilt Policies

##### Count Based

Here you can specify how many elements you need to be included in the window. 

Here you can specify the Tumbling window count size as follows and it implicitly handles the 
windowing logic. 

```java
 BaseWindowSink dw = new DirectWindowedReceivingTask()
        .withTumblingCountWindow(5);   

```

Here you use the inbuilt windowing policies. 



##### Duration Based

In here you can specify, how much time should wait to gather elements for a window. 
Here we can specify the timing unit type and the unit size to sepcify it. 

```java
BaseWindowSink dwDuration = new DirectWindowedReceivingTask()
        .withTumblingDurationWindow(2, TimeUnit.MILLISECONDS);
```

Here you use the inbuilt windowing policies. 

#### Custom Policies

If you want to design a custom logic you can use the following way to design the example. 

Here you can specify a window type. This window type can be designed by using existing policies or
you can create your own policies. Here withWindow functionality allows you to add a custom
windowing policy. If you want to design your own policy, you need to extend from BaseWindow
class and create your own policies. And these policies are supported by the windowing stratergies. 
For instance check TumblingCountWindowStratergy or TumblingDurationWindowStratergy  which extends 
BaseWindowStratergy. Like this you can
create your own windowing policies. 

Count based, 

```java
 BaseWindowedSink dw = new DirectCustomWindowReceiver()
        .withWindow(TumblingCountWindow.of(5));    
```


Duration based,

```java
BaseWindowedSink dwDuration = new DirectCustomWindowReceiver()
        .withWindow(TumblingDurationWindow.of(2));
```

### Running Examples


#### Inbuilt Policy

```bash
./twister2-0.2.1/bin/twister2 submit standalone jar twister2-0.2.1/examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 500 -workers 4 -size 8 -op "direct" -stages 4,4 -verify -stream -window
```

#### Custom Policy

```bash
./twister2-0.2.1/bin/twister2 submit standalone jar twister2-0.2.1/examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 500 -workers 4 -size 8 -op "cdirect" -stages 4,4 -verify -stream -window
```