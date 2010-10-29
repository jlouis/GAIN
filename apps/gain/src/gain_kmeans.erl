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

-export([kmeans_step/3]).
-compile(export_all).

-define(STOP_EPSILON, 0.1). %% Rather arbitrary at the moment
%%====================================================================
%% API
%%====================================================================

%% @doc Run (Voronoi-)k-means on Bucket through connection C with N clusters.
%% @end
-type kmeans_vector() :: [{binary(), float()}].
-spec kmeans(pid(), binary(), integer()) -> kmeans_vector().
kmeans(C, Bucket, N) ->
    Clusters = initialize_clusters(Bucket, N),
    kmeans_iterate(C, Bucket, Clusters, N).

%% @doc For a row Object, return the column names for that row
%%  Select the column names and return them as a set
%% @end
map_get_dimension_names(Obj, _, _) ->
    L = binary_to_term(riak_object:get_value(Obj)),
    Dims = [D || {D, _} <- L],
    [sets:from_list(Dims)].

%% @doc Find the Cluster an Obj belongs to
%%  Assume that the object contains a sparse vector of affinities.
%%  then this map finds the cluster nearest to the Obj. The function will
%%  return a singleton list with the cluster number, a count of one and the
%%  vector for the object.
%% @end
map_sort_vector(Obj, Cls) ->
    map_sort_vector(Obj, undefined, Cls).

map_sort_vector(Obj, _, [Clusters]) ->
    Vec = binary_to_term(riak_object:get_value(Obj)),
    N = find_nearest_cluster(Vec, Clusters),
    [{N, 1, Vec}]. % Tell that we are in cluster N

%% @doc Gather Objects into Clusters
%%  Given a list of Objects with their Cluster Numbers designated,
%%  reduce them by combining vectors in the same cluster. Returns the
%%  combined clusters with their count so the next cluster iteration can
%%  be calculated.
%% @end
red_calc_new_clusters(L, _) ->
    Sorted = lists:keysort(1, L),
    fold_clusters(Sorted).

%% @doc Union a List of sets
%% @end
red_set_union(List, _) ->
    Union = sets:union(List),
    [Union].

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Sum two sparse vectors
%%  given as {D, V} pairs, where D is the the dimension name and V is the
%%  sum of that dimension.
%% @end
sum_vec([{K1, V1} | R1], [{K2, V2} | R2])
  when K1 == K2 ->
    [{K1, V1 + V2} | sum_vec(R1, R2)];
sum_vec([{K1, V1} | R1], [{K2, V2} | R2])
  when K1 < K2 ->
    [{K1, V1} | sum_vec(R1, [{K2, V2} | R2])];
sum_vec([{K1, V1} | R1], [{K2, V2} | R2])
  when K1 > K2 ->
    [{K2, V2} | sum_vec([{K1, V1} | R1], R2)].

fold_clusters([]) -> [];
fold_clusters([X]) -> [X];
fold_clusters([{CLNo, Count, Vec}, {CLNo2, Count2, Vec2} | Rest])
  when CLNo == CLNo2 ->
    fold_clusters([{CLNo, Count + Count2, sum_vec(Vec, Vec2)} | Rest]);
fold_clusters([{CLNo, Count, Vec}, {CLNo2, Count2, Vec2} | Rest])
  when CLNo =/= CLNo2 ->
    [{CLNo, Count, Vec} | fold_clusters([{CLNo2, Count2, Vec2} | Rest])].

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
    {T, NewClusters} =
	timer:tc(gain_kmeans, kmeans_step, [C, Bucket, Clusters]),
    case kmeans_difference(Clusters, NewClusters) of
	F when F < ?STOP_EPSILON ->
	    NewClusters;
	F ->
	    ?DEBUG([iterating, [{time, T / (1000*1000)}, {distance, F}]]),
	    kmeans_iterate(C, Bucket, NewClusters, N)
    end.

kmeans_difference(Old, New) ->
    lists:sum([euclidian_distance(V1, V2) || {V1, V2} <- lists:zip(Old, New)]).

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
    NC = lists:keysort(1, NewClusters),
    F = fun(Count, Vec) ->
		[V / Count || V <- Vec]
	end,
    [F(Cnt, Vec) || {_, Cnt, Vec} <- NC].

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


