---
id: channels
title: Channels
sidebar_label: Channels
---

Twister2 creates a communication network between workers(processes) at runtime to enable message passing between workers. Twister2 can be configured to use different transports and interfaces to optimize the communication layer based on the hardware support of your cluster.

1. TCP/IP

Twister2 can be configured as follows in network.yaml file to use TCP/IP without using any 3rd party message passing interface underneath.

```yaml
twister2.network.channel.class: "edu.iu.dsc.tws.comms.tcp.TWSTCPChannel"
```

In this mode, twister2 will use java nio based Socket Channels to build the communication network.

2. OpenMPI

```yaml
twister2.network.channel.class: "edu.iu.dsc.tws.comms.mpi.TWSMPIChannel"
```

When twister2 is configured as above in network.yaml file, communication layer will use MPI's iSend and iRecv to communicate between worker nodes. Underneath, MPI can be configured to use any protocol at BTL(Byte Transfer Layer). Check [OpenMPI doc](https://www.open-mpi.org/doc/) for more details.

3. Open UCX

```yaml
twister2.network.channel.class: "edu.iu.dsc.tws.comms.ucx.TWSUCXChannel"
```

network.yaml can be configured as above to use [open UCX](https://www.openucx.org/), at communication layer. With open UCX, twister2 get the capability to work on following transports.

### Transports

* Infiniband
* Omni-Path
* RoCE
* Cray Gemini and Aries
* CUDA
* ROCm
* Shared Memory
* posix, sysv, cma, knem, and xpmem
* TCP/IP