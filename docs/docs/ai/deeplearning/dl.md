---
id: deeplearning
title: Deep Learning
sidebar_label: Overview on Deep Learning
---

Twister2 Deep Learning is currently working on supporting Pytorch, Tensorflow and MXNet. 
With our 0.5.0 release we have introduced an experimental version of a deep learning with
 distributed data parallel Pytorch implementation. We use an MPI backend to support distributed
  training and currently we are working on providing better performance with GPU.

# Twister2:DeepNet

Twister2:DeepNet is the distributed runtime for deep learning algorithm training and inference. 
Currently we support an experimental version of a distributed training for Pytorch. Our initial 
implementation uses Apache Arrow and Parquet to move data from our data pre-processing pipeline to
the data training framework. We are actively working on transforming this process into a seamless
in-memory computation model. 



