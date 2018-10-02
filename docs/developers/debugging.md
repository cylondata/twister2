# Debugging Twister2

Because Twister2 is a distributed framework debuging the code to pin point bugs can be challenging.
The guide below describes how you can debug the Twister2 using a remote debugger. This guide is for
debugging on your locally machine. There may be other steps that you need to take if you are debugging
an Twister2 which is running in a different server or machine.

## Editing the scripts
In the distribution you will find a ```conf``` folder. Under the conf folder you will see separate
directories for each type of resource managers that are supported. If you are running on your local
machine you would most probably be using  ```standalone```. Which means you are running your application
with a command like below (which is for the hello world example)

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

Notice that it has ```submit nodempi``` which indicate that you are using the ```standalone``` resource
allocation mechanism. Once you go into the ```standalone``` folder (The folder structure and the files
should be the same for each resource manager), you will see a file nameed ```exp.sh```. This file has
the following segment of script in it.

```bash
if [ $OMPI_COMM_WORLD_RANK = "0" ]; then
    profile=-agentpath:/home/supun/tools/jprofiler7/bin/linux-x64/libjprofilerti.so=port=8849,nowait
    debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006
fi

profile=
debug=
```
In order to debug the code just comment out the line ```debug=```. Another thing to note that is the 
line ```$OMPI_COMM_WORLD_RANK = "0"```, this line states that the debugger will be applied to the
worker that has rank '0'. For example if you are running an application which runs 4 worker instances
they will be numbered (given ranks) from 0 to 3. Since the value is set to '0' you will debugging the
zeroth worker. If you want to debug some other worker you can change the value to what you want.

## Configuring your IDE

After the scripts have been properly edited you need to setup an remote debugger on your preferred
IDE. The explanation below is based in Intellij IDEA. Under ```Run``` you can go to ```Edit Configurations```
and click the ```+``` sign and select the ```remote``` debugger option. There you will see a line 
similar to the following

```bash
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006
```

make sure the address variable has the same value as the address variable in the ```exp.sh``` script
that was mentioned above. 

## Start Debugging 

Once all the configurations are set. Start the Twister2 application. After a little while the application
will hang waiting for you to start the debugger. Then you can start the debugger in your IDE and it will
attach to the Twister2 application. 

