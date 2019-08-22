---
id: configurations
title: Twister2 Configurations
sidebar_label: Configurations
---

A detailed description of all the configuration parameters supported by Twister2 can be found in [Twister2 Configurations](/configs).

## Configuration Folder

Twister2 is configured using set of YAML files. These configurations are related to different 
components of Twister2. All the configurations can be found inside the ```conf``` folder of Twister2
distribution. Below shows the sample structure of the configuration folder

<article>
<img align="left" src="/docs/assets/conf_dir.png">
</article>


Inside there you'll find several folders. Below table describes these folders along with
the configurations inside them.
 
| Directory | Description |
| :--- | :--- | 
| common      | Common configurations to all the resource schedulers |
| standalone | Standalone deployment mode specific configurations | 
| kubernetes | Kubernetes based deployment configurations | 
| mesos | Mesos based deployment configurations |
| nomad | Nomad based deployment configurations |
| slurm | Slurm based deployment configuration | 
| dashboard | UI Configurations | 

When reading configurations, Twister2 first looks at the ```common``` folder.
Then it looks at deployment specific folder for configurations. One can specify the same configuration parameter in a file in
```common``` folder as well as deployment specific folder. The configurations read from the deployment 
specific folder takes precedent over ```common``` configurations. If the same parameter is specifed in two files under the same 
folder the order is not specified.

## Configuration Files

In ```common``` and deployment specific folders we can find the following files.  

| Config File | Description |
| :--- | :--- | 
| core.yaml      | Core services related configurations, job master, thread pools etc |
| resource.yaml | Resource provisioning and job submit related configurations | 
| network.yaml | Network and parallel operator related configurations | 
| task.yaml | Compute API related configurations including, task scheduler and executor |
| data.yaml | Data access related configurations |
| checkpoint.yaml | Check-pointing configurations | 

A detailed description of the configurations can be found in the following guide.

[Twister2 Configurations](/configs).

