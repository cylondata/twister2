---
id: twister2flow
title: Twister2Flow
sidebar_label: Overview on Twister2Flow
---

# Twister2Flow

Designing a dataflow pipeline to support high performance deep learning in Twister2. This is an experimental version of the initial High Performance Deep Learning Connect of Twister2. 

## Supporting Dataflow Operations

1. File Systems oriented dataflow operations
2. Remote Memory Access (Work In Progress)

## Supporting Frameworks

1. Twister2 (JVM Oriented Big Data Toolkit)
2. Pytorch (Deep Learning Library)
3. Python3

## Install

```bash
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps twister2flow-test
```


## Bootstrap Task Executor

```bash
python3 bootstrap/PytorchJobSubmitter.py\ 
    --script /home/vibhatha/github/forks/twister2/deeplearning/pytorch/src/main/python/PytorchMnistDist.py\ 
    --executor /home/vibhatha/venv/ENV37/bin/python3\ 
    --parallelism 4 --hostfile hostfile
```

## Example Dataflow


```python3
from twister2flow.twister2.pipeline import PipelineGraph
from twister2flow.twister2.task.Twister2Task import Twister2Task
from twister2flow.twister2.task.PytorchTask import PytorchTask
from twister2flow.twister2.task.PythonTask import PythonTask

plg = PipelineGraph.PipelineGraph(name="UNNAMED_TASK")

download_task = PythonTask(name="download_task")
download_task.set_command("python3")
download_task.set_script_path(script_path="MnistDownload.py")
download_task.set_exec_path(exec_path=None)

twister2_task = Twister2Task(name="t2_task")
twister2_task.set_command("twister2 submit standalone python")
twister2_task.set_script_path(script_path="Twister2PytorchMnist.py")
twister2_task.set_exec_path(exec_path=None)

pytorch_task = PytorchTask(name="pytorch_task")
pytorch_task.set_command()
pytorch_task.set_script_path(script_path="PytorchMnistDist.py")
pytorch_task.set_exec_path(exec_path=None)

plg.add_task(download_task)
plg.add_task(twister2_task)
plg.add_task(pytorch_task)


print(str(plg))

plg.execute()
```

Running example

```python
python3 examples/Twister2Flow.py
```


