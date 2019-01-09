#Joins

## Introduction

Joins are a important operation that is windley used in big data applications. Since joins can be
very expensive operation it is important to have a efficient join operation. Twister2 supports join operations through its optimized communication layer. 

The following join example showcases how a join operation can be performed using the Twister2 communication API. Since this example is presented at the communication level the complexity of the code is a little high. Since the code needs to handle other aspects such as task management and execution within the example code. Using joins at the task layer and data layer will gradually reduce this complexity because each layer hides complexities from the end user. However it is usefull to understand operations at the communication layer since it gives you the 
most freedom to optimize according to your needs. 

## Data

The data set used for this example is pretty simple and straightforward. The example joins two data sets, the first one has a set of student id's and the corresponding first names for each student id. And the second data set has student id's and the corresponding courses that students have taken. 

#### Data Set 1
| Student Id | First Name |
|:----------:|:----------:|
|      1     |    John    |
|      2     |    Matt    |
|      3     |    Kara    |

#### Data Set 2

| Student Id | Course Id  |
|:----------:|:----------:|
|      1     |    E101    |
|      2     |    E351    |
|      3     |    E403    |
|      1     |    E403    |

#### Resulting Data set

| Student Id | First Name |  Course Id |
|:----------:|:----------:|:----------:|
|      1     |    John    | E101, E403 |
|      2     |    Matt    |    E351    |
|      3     |    Kara    |    E403    |


## Running join example

The join example is added in the Twister2 code as a communication example. The code has comments explaining each step of the example to explain what each section of the code does. You can use the following command to execute the join example. 

Full Code example - [Student Join Example](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/comms/batch/BJoinStudentExample.java)

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -workers 8 -op "joinstudent" -stages 8,1 2>&1 | tee out.txt
```

As a result at the end of the run the following will be printed which show the joined results. Please note that the results generated are based on the values in the example, which are different from the simple example provided above.

```bash
Key 1 : Value [John, E342, E247, E333]  
Key 2 : Value [Peter, E542]  
Key 3 : Value [Tedd, E242, E101]  
Key 4 : Value [Jake, E342]  
Key 5 : Value [Matt, E347, E541]  
Key 6 : Value [Adam, E347]  
Key 7 : Value [Max, E101]  
Key 8 : Value [Roger, E241]  
```

## How it works underneath.

The join is implemented using two keyed partition operations, The framework shuffles the data to sink tasks based on the key values such that data entries with the same key values are collected at a single sink task. Then the appropriate join operation is executed based on the keys and values in both data sets and the combined results are sent back to the user.