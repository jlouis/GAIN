%%%-------------------------------------------------------------------
%%% File    : gain_kmeans.erl
%%% Author  : Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%% Description : A Kmeans implementation
%%%
%%% Created : 29 Oct 2010 by Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%%-------------------------------------------------------------------
-module(gain_kmeans).

-include("log.hrl").

%% API
-export([kmeans/3]).

%% Internal
-export([map_get_dimension_names/3, red_set_union/2,
	 map_sort_vector/3, red_calc_new_clusters/2]).

-define(STOP_EPSILON, 0.1). %% Rather arbitrary at the moment
%%====================================================================
%% API
%%====================================================================
-type kmeans_vector() :: [{binary(), float()}].
-spec kmeans(pid(), binary(), integer()) -> kmeans_vector().
kmeans(C, Bucket, N) ->
    Clusters = initialize_clusters(Bucket, N),
    kmeans_iterate(C, Bucket, Clusters, N).

map_get_dimension_names(Obj, _, _) ->
    L = binary_to_term(riak_object:get_value(Obj)),
    Dims = [D || {D, _} <- L],
    [sets:from_list(Dims)].

map_sort_vector(Obj, _, Clusters) ->
    Vec = binary_to_term(riak_object:get_value(Obj)),
    N = find_nearest_cluster(Vec, Clusters),
    [{N, Vec}]. % Tell that we are in cluster N

red_calc_new_clusters(_Sorted, _Clusters) ->
    todo.

red_set_union(List, _) ->
    Union = sets:union(List),
    [Union].

%%====================================================================
%% Internal functions
%%====================================================================
find_nearest_cluster(Vec, [C | Clusters]) ->
    D = euclidian_distance(Vec, C),
    find_nearest_cluster(Vec, Clusters, 1, {0, D}).

find_nearest_cluster(_Vec, [], _K, {N, _D}) -> N;
find_nearest_cluster(Vec, [Clus | Rest], K, {N, D}) ->
    D2 = euclidian_distance(Vec, Clus),
    case D2 < D of
	true ->
	    find_nearest_cluster(Vec, Rest, K+1, {K, D2});
	false ->
	    find_nearest_cluster(Vec, Rest, K+1, {N, D})
    end.

%% Discrete metric for now.
euclidian_distance(Vec, Clus) ->
    Q = euclidian_distance(Vec, Clus, 0),
    math:sqrt(Q).

euclidian_distance([], [], Q) -> Q;
euclidian_distance([{C1, V1} | R1], [{C2, V2}, R2], Q)
  when C1 == C2 ->
    R = (V1 - V2) * (V1 - V2),
    euclidian_distance(R1, R2, Q + R);
euclidian_distance([{C1, V1} | R1], [{C2, V2} | R2], Q)
  when C1 < C2 ->
    R = (V1 - 0) * (V1 - 0),
    euclidian_distance(R1, [{C2, V2} | R2], Q + R);
euclidian_distance([{C1, V1} | R1], [{C2, V2} | R2], Q)
  when C1 > C2 ->
    R = (0 - V2) * (0 - V2),
    euclidian_distance([{C1, V1} | R1], R2, Q + R).

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

kmeans_iterate(C, Bucket, Clusters, N) when length(Clusters) < N ->
    %% If nobody are sorted into a cluster, reset it and try again
    NewCls = create_clusters(Bucket, N - length(Clusters)),
    kmeans_iterate(C, Bucket, NewCls ++ Clusters, N);
kmeans_iterate(C, Bucket, Clusters, N) ->
    NewClusters = kmeans_step(C, Bucket, Clusters),
    case kmeans_difference(Clusters, NewClusters, 0) of
	F when F < ?STOP_EPSILON ->
	    NewClusters;
	F ->
	    ?DEBUG([iterating, {distance, F}]),
	    kmeans_iterate(C, Bucket, NewClusters, N)
    end.

kmeans_difference([], [], Diff) -> Diff;
kmeans_difference([{_, D1} | R1], [{_, D2} | R2], Diff) ->
    kmeans_difference(R1, R2, Diff + abs(D1 - D2)).

kmeans_step(C, Bucket, Clusters) ->
    {ok, [{1, NewClusters}]} =
	riakc_pb_socket:mapred_bucket(
	  C, Bucket,
	  [{map,
	    {modfun, gain_kmeans, map_sort_vector},
	    [Clusters], false},
	   {reduce, {modfun, gain_kmeans, red_calc_new_clusters},
	    undefined, true}]),
    step_post_process(NewClusters).

step_post_process(NewClusters) ->
    NewClusters.

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


