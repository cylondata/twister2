##Configuring

Update ```tests/base.json```

```json
{
  "jarRootDir": "Root directory for Jars.",
  "t2Bin": "Path to Twister2 binary file"
}
```

For a particular benchmark, if we need to pick jar from a different directory, 
that can be done by setting following parameters in the benchmark definition.

```json
{
"directory": {
    "relativeToRoot": false,
    "path": "Path to Jar"
  }
}
```

## Running All Benchmarks

```python launcher.py```

## Running a Subset of Benchmarks

```python launcher.py comms_reduce,comms_gather```