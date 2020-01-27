---
id: deeplearning
title: Overview
sidebar_label: Overview
---

# Twister2:DeepNet

Twister2:DeepNet is the distributed runtime for deep learning algorithm training and inference. 
Currently we support an experimental version of a distributed training for Pytorch. Our initial 
implementation uses Apache Arrow and Parquet to move data from our data pre-processing pipeline to
the data training framework. We are actively working on transforming this process into a seamless
in-memory computation model. 


![NeuralNet-Brain: Reference: http://rpg.ifi.uzh.ch/research.html](assets/dl-brain-1.gif)

Reference: http://rpg.ifi.uzh.ch/research.html

