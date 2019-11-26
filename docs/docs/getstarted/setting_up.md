---
id: setup_twister2
title: Setup Twister2
sidebar_label: Setup Twister2
---

For the examples in this section you can use the Twister2 docker image or compile twister2 on your own. 

We use the standalone mode of Twister2 to deploy the applications.

<!--DOCUSAURUS_CODE_TABS-->
<!--Docker-->

```bash

# get the docker image from the repository
docker pull twister2/standalone

# run the docker image and log into it
docker run -it twister2/standalone bash

# go to the pre-compiled twister2 
cd twister2-0.4.0

# run your first twister2 application
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 2
```

<!--Compile on laptop-->

Here are the essential steps in compiling Twister2.

```bash
wget https://github.com/DSC-SPIDAL/twister2/archive/0.4.0.tar.gz
tar -xvf twister2-0.4.0.tar.gz

cd twister2-0.4.0
./build_linux.sh
```

The above command will install Twister2 along with system dependencies. You will need to give sudo
access to the script. It will take about 20 minutes to build Twister2 depending on the system
configuration. 

The above command will build a twister2 distribution and we need to extract it.

```bash
# the binary is created inside this package
cd bazel-bin/scripts/package
tar -xvf twister2-0.4.0.tar.gz
# go inside the twister2 binary
cd twister2-0.4.0

# run your first twister2 application
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 2
```

[Compiling Twister2](../compiling/compiling.md) explains the steps in detail. After building the code and
following the instructions in [Twister2 Distribution](../compiling/linux.md) you should have a extracted 
folder named `twister2-0.4.0`, this would be your twister2 home folder.

<!--END_DOCUSAURUS_CODE_TABS-->

The HelloWorld example, starts set IWorkers and they all print the message. Here is an example message
from 1st worker. 

```bash
[INFO] [worker-1] [main] edu.iu.dsc.tws.examples.basic.HelloWorld: Hello World from Worker 1; there are 2 total workers and I got a message: Twister2-Hello
```

Congratulations! You ran your first twister2 application! 


###

