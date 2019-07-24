---
id: concepts
title: Concepts Overview
sidebar_label: Concepts
---

This document describes the parallel and distributed concepts applied to Twister2.

## Compute Resources

In general a Twister2 Application executes on a distributed set of nodes in a cluster. 
These nodes may consist of physical machines, virtual machines or docker container.

From Twister2 point of view each of these nodes have the following components.

1. One or more CPUs (Usually we call one CPU a socket)
2. Each CPU can have one or more cores (a core is a processing unit)
3. Random access memory (possibly in Numa configuration)
4. One or more disks
5. Connected to a TCP network
6. Optionally connected to a high performance network such as Infiniband or Omni-Path

Goal of Twister2 is to run a users defined application in such a distributed environment

## Twister2 Job

A user writes the source code of a Twister2 Job using many API's availble to them through Twister2 API's.