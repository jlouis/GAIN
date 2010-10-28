%%%-------------------------------------------------------------------
%%% File    : gain_kmeans.erl
%%% Author  : Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%% Description : A Kmeans implementation
%%%
%%% Created : 29 Oct 2010 by Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%%-------------------------------------------------------------------
-module(gain_kmeans).

%% API
-export([kmeans/2]).

%% Internal
-export([map_get_dimension_names/3, red_set_union/2]).

%%====================================================================
%% API
%%====================================================================
-type kmeans_vector() :: [{binary(), float()}].
-spec kmeans(binary(), integer()) -> kmeans_vector().
kmeans(Bucket, N) ->
    Clusters = initialize_clusters(Bucket, N),
    kmeans_iterate(Bucket, Clusters, N).

map_get_dimension_names(Obj, _, _) ->
    L = binary_to_term(riak_object:get_value(Obj)),
    Dims = [D || {D, _} <- L],
    [sets:from_list(Dims)].

red_set_union(List, _) ->
    Union = sets:union(List),
    [Union].

%%====================================================================
%% Internal functions
%%====================================================================
create_random_cluster([]) -> [];
create_random_cluster([X | Dims]) ->
    R = crypto:rand_uniform(0, 1000*1000*1000) / (1000 * 1000 * 1000),
    [{X, R * 2 - 1} | create_random_cluster(Dims)].

create_clusters(0, _Dims) -> [];
create_clusters(N, Dims) when N > 0 ->
    [create_random_cluster(Dims) | create_clusters(N-1, Dims)].

initialize_clusters(Bucket, N) ->
    Dimensions = find_dimensions(Bucket),
    create_clusters(N, Dimensions).

kmeans_iterate(Bucket, Clusters, N) when length(Clusters) < N ->
    NewCls = create_clusters(Bucket, N - length(Clusters)),
    kmeans_iterate(Bucket, NewCls ++ Clusters, N);
kmeans_iterate(_Bucket, _Clusters, _N) ->
    todo.

find_dimensions(Bucket) ->
    {ok, RC} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    {ok, [{1, [Set]}]} =
	riakc_pb_socket:mapred_bucket(
	  RC, Bucket,
	  [{map,
	    {modfun, gain_kmeans, map_get_dimension_names},
	    undefined, false},
	   {reduce, {modfun, gain_kmeans, red_set_union},
	    undefined, true}]),
    lists:sort(sets:to_list(Set)).

