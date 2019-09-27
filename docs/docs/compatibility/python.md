---
id: pythonapi
title: Python API
sidebar_label: Python API
---

The Python API provides the ability to define Twister2 TSet jobs in python.

## Getting Started

### Following components are required in your system.

1. Python 3.7
2. JVM 8+

### Setting up twister2 python support

```bash
pip3 install twister2
```

## Writing your first application

### Twister2 Environment

```python
env = Twister2Environment(name="MyPython", config={"sample_config": 123}, resources=[{"cpu": 1, "ram": 1024, "instances": 2}])
```

Twister2Environment can be considered as the entry point to connect to the java runtime. Twister2Environment optionally allows you to configure your job and resources.

### TSet Source

TSet is all about data transformations. Hence every TSet workflow starts with a source.

TSet source in python should follow below skeleton.

```python
class SourceFunc(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def has_next(self):
        return False

    @abstractmethod
    def next(self):
        return None
```

Shown below is a simple integer source created based on above skeleton.

```python
from twister2.tset.fn.SourceFunc import SourceFunc

class IntegerSource(SourceFunc):

    def __init__(self, limit):
        super(IntegerSource, self).__init__()
        self.x = 0
        self.limit = limit

    def has_next(self):
        return self.x < self.limit

    def next(self):
        self.x += 1
        return self.x
```

Once defined, you may add this source to TSet environment as follows.

```python
source = env.create_source(IntegerSource(10), 4)
```

The second parameter of ```create_source``` is the parallelism. In this case(4), twister2 will run 4 integer sources parallelly across the cluster.

Starting from a source, you may transform your data as required by utilizing twister2 TSet operations.

### Example - KMeans

This example utilizes existing tools for python such as ```numpy``` and ```scikit-learn``` for data representation and computation and use twister2 for communication.

```python
# Twister2 Python API supports Numpy
import numpy as np

from twister2.TSetContext import TSetContext
from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc

env = Twister2Environment(resources=[{"cpu": 4, "ram": 4096, "instances": 1}])


class PointSource(SourceFunc):

    def __init__(self, size, count):
        self.i = 0
        self.read = True
        self.size = size
        self.count = count

    def has_next(self):
        return self.read

    def next(self):
        self.i = self.i + 1
        arr = np.random.rand(self.count, self.size)
        self.read = False
        return arr


data = env.create_source(PointSource(100, 10000), 10).cache()
centers = env.create_source(PointSource(100, 200), 10).cache()


def apply_kmeans(points, ctx: TSetContext):
    # If you need to use a 3rd party library, you should import them within the function body.
    from sklearn.cluster import KMeans
    c = ctx.get_input("centroids")
    centers = c.get_partition(ctx.get_index()).consumer().__next__()
    kmeans = KMeans(init=centers, n_clusters=200, n_init=1).fit(points)
    return kmeans.cluster_centers_

# Most of the TSet operations accept a function. You can even pass a lambda instead!
mapped = data.direct().map(apply_kmeans)


def reduce_centroids(c1, c2):
    return c1 + c2


def average(points, ctx):
    import numpy as np
    return np.divide(points, 10)


reduced = mapped.all_reduce(reduce_centroids).map(average)

for i in range(10):
    mapped.add_input("centroids", centers)
    centers = reduced.cache()

centers.direct().for_each(lambda x: print(x))
```

## Submitting Python Jobs to Twister2

Twister2 accepts two types of python jobs.

1. Single python file (If you have just one file defining the whole job)
```bash
./bin/twister2 submit standalone python /absolute/path/to/your_script.py your_script.py  arg1 arg2
```

2. Zip file (If you have multiple python files, you can submit them as a zip file)
```bash
./bin/twister2 submit standalone python_zip /absolute/path/to/your_zip.zip entry_point.py  arg1 arg2
```

```entry_point.py``` is where you would initialize Twister2Environment

```arg1``` and ```arg2``` in above two commands will be passed as system args to the python script.
