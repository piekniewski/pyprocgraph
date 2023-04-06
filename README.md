## Python Process Graph

Pyprocgraph is a package providing abstractions and 
classes to make it easy to express multiprocessing
pipelines. This is useful when e.g. some continuous
stream of data needs to be processed in stages via 
several distinct procedures. Pyprocgraph makes it easy
to construct such computational graphs connected and 
synchronized using multiprocessing queues. 

### Basics

The package introduces two main abstractions - a worker and 
an executor. 

#### Workers

A worker is a class that derives from process
which implements some function. A worker will typically have
inputs and outputs in the form of multiprocessing
queues and will perform a certain transformation when
a new item it sent over an input queue. 

#### Executors

An executor is an objects that takes a set of workers
connected using queues and orchestrates the execution. 
The default executor will take care our launching and 
monitoring if any workers in the graph had exited. The user
can also extend executor into a custom class to provide
workes with custom shared memory objects and additional 
functionality.

### Installing

Install directly from github using pip:

```pip install git+https://github.com/piekniewski/pyprocgraph.git#egg=pyprocgraph```

or first close the repo and then install for edit:

```git clone https://github.com/piekniewski/pyprocgraph.git```

```cd pyprocgraph; pip install -e . ```