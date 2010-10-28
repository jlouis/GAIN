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

     reddit_rec:read_file("reddit/affinities.erl.dump").

in the erlang shell to dump the reddit data in a bucket
<<"affinities">> inside riak. At the moment, this where we are.

