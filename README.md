Location Entropy
================

### Prerequisite

* Python 2.7
* Spark >= 2.3.0
* Jupyter notebook

Run `bash install.sh` to set up and perform the check for you.

### Run

To rerun the jupyter notebook line by line, type the command below.

```
$ PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS='notebook' pyspark --master local[4]
```

### Unit test

```
$ python -m unittest test_utils
```

Test coverage: 100%

### Discussion

Here are some additional discussions from an engineering perspective.

###### Data skew and its impact

In a production environment, the source data probably comes as
`events` in the form of "which user visits which location",
instead of aggregated data in the form of
"how many times a place has been visited by unique users".
In this case, popular locations will inevitably emits more events
than others.

However, such data skew will _unlikely_ undermine the performance of
the data pipeline as Apache Spark is very efficient on group-by-count operations
by performing map-side reduction.
From my past experience,
when counting clicks and views from user activity logs on TeraBytes scale,
Spark is very efficient,
provided that I know there were skews in those logs.
In the case of calculating location entropy,
data skew should not be a primary concern.

### Scala version

Frankly speaking, though having followed through a Scala course on Coursera,
I am not very proficient in Scala and
all my ETL and pipelines are written in Python 2.7.
However I tried to produce a Scala version
which is available in [scala](/scala) directory.
