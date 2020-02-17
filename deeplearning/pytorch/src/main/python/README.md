# README

## Python Setup 

To Run the code natively while doing development set the python path. 

The following is an example on how to do that;

Add the following line to your ~/.bashrc file. 

```bash
vim ~/.bashrc
```

Add in the bottom. 

```bash
ESC + G + A
```

```bash
export PYTHONPATH=/home/vibhatha/github/forks/twister2/deeplearning/pytorch/src/main/python
```

## Examples

There is a program written with Both Twister2 Python API and Twister2 DeepNet API. 
The following script shows how to run the example. But make sure, you have set the 
PYTHONPATH properly. 

```bash
./bin/twister2 submit standalone python deeplearning/pytorch/src/main/python/MnistDistributedExampleTest.py
```