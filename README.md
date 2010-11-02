# GAIN - Artificial Intelligence Network

GAIN is a project in progress. The goal is to create a tool akin to
Apache Mahout, but written around Erlang rather than Java. The initial
focus is to understand and implement whatever algorithm there is in
Mahout and then later look at performance, speed, and so on.

# Starting out

This is a fast startup description written by jlouis so we can get
people up to speed rather quickly.

## Getting and running riak.

We have initially started with the riak k/v store as the data
backend. Riak is available from github at this
[link](http://github.com/basho). To install riak, just follow their
basic instructions which will create an embedded release in the rel
subdirectory. At the moment we simply then start a riak node at
riak@127.0.0.1, like their hints suggest.

There is one specific configuration parameter that has to be set at
the moment on riak. This is the following:

      {vnode_cache_entries, 0},

which disables the riak-internal LRU cache completely. We have seen
a number of problems with this internal caching system, so we recommend
it to be turned off at the time being.

## Getting the reddit raw data.

For now, see the README.md file in the reddit/ subdirectory. Then run
'make' in that directory to generate a simplified vote database we use
to load the data into riak. Note we are calling sort with a memory
usage of 400 megabytes, so you need some free memory to do the sorting
step.

## Loading data into riak.

First, we assume you already have a riak instance up and running. And
that you have tested it for the basics of storing data and retrieving
data from it.

Then you run

     make deps # Fetch dependencies
     make rel

which creates a release of gain in the rel subdirectory. Now you can
run

     rel/gain/erts-<vsn>/bin/erl -pa apps/gain/ebin

giving you an erlang containing the gain application as well as the
riakc protocol buffers client. The '-pa' addition of the ebin path is
not strictly necessary, but it makes it easier to 'l(Mod)' from the
shell to get the latest version of a module when you compile it in the
app.

At this point, try to execute the command

     reddit_rec:read_file("reddit/affinities.erl.small.dump").

in the erlang shell to dump the reddit data in a bucket
<<"affinities">> inside riak. At the moment, this where we are.

# Test cases

We have a target for running test cases. At the moment, we are just
using eunit tests in modules when we need to do some tests. The target
to run is,

     make eunit

which will run all eunit tests.

# K-Means

We have a preliminary implementation of a (Voronoi-) K-means
algorithm. We assume that a bucket 'B' exists. *B* contains *{K, V}*
objects where *K* is a row name designator and *V* is a sparse vector
stored as a list *[{name(), float()}]* where we name each dimension
and supply a floating point value in the range *-1* to *1*. Note that
we take as a *precondition* that the names are **ordered** according
to the Erlang standard ordering operator *<*.

K-means then run in a step-iteration
[algorithm](http://en.wikipedia.org/wiki/K-means_clustering#Standard_algorithm)
based upon the idea of
[Lloyd](http://en.wikipedia.org/wiki/Lloyd's_algorithm). Note that the
first step is to sort each *{K, V}* pair into the cluster closest to
the pair. The second step is to calculate a new mean for the cluster
based upon its members.

The first step is a map in a Map/Reduce implementation: Simply for
each pair, check all clusters and report the one it is closest to. The
second step is a reduce step: Gather up elements belonging to each
cluster and calculate the mean. Note that several nice properties of
the mean w.r.t. commutativity make it possible to form a reduction
tree.

Finally, to initialize the algorithm, produce K clusters by randomly
assigning values in the N-dimenional space I^N where I is the interval
[-1:1]. Iterate until the K clusters differ from the earlier iteration
by a value less than some small epsilon.

This idea has been implemented in *gain_kmeans.erl*.

### Testing

Assume the data above has been loaded from reddit into the bucket
*<<"affinities">>*. Then we can ask for a kmeans run on these data via
the command

     reddit_rec:kmeans().

which will try to run a K-means computation on the reddit data.
