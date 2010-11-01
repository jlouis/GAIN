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
-type kmeans_vector(A) :: [{A, float()}].
-spec kmeans(pid(), binary(), integer()) -> kmeans_vector(term()).
kmeans(C, Bucket, N) ->
    Clusters = initialize_clusters(C, Bucket, N),
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
    fold_clusters(L).

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
-spec sum_vec(kmeans_vector(A), kmeans_vector(A)) -> kmeans_vector(A).
sum_vec([], Component2) -> Component2;
sum_vec(Component1, []) -> Component1;
sum_vec([{K1, V1} | R1], [{K2, V2} | R2])
  when K1 == K2 ->
    [{K1, V1 + V2} | sum_vec(R1, R2)];
sum_vec([{K1, V1} | R1], [{K2, V2} | R2])
  when K1 < K2 ->
    [{K1, V1} | sum_vec(R1, [{K2, V2} | R2])];
sum_vec([{K1, V1} | R1], [{K2, V2} | R2])
  when K1 > K2 ->
    [{K2, V2} | sum_vec([{K1, V1} | R1], R2)].

-spec fold_clusters([{integer(), integer(), kmeans_vector(A)}]) ->
        	    [{integer(), integer(), kmeans_vector(A)}].
fold_clusters(L) ->
    Sorted = lists:keysort(1, L),
    fold_clusters_sorted(Sorted).

fold_clusters_sorted([]) -> [];
fold_clusters_sorted([X]) -> [X];
fold_clusters_sorted([{CLNo, Count, Vec}, {CLNo2, Count2, Vec2} | Rest])
  when CLNo == CLNo2 ->
    fold_clusters_sorted([{CLNo, Count + Count2, sum_vec(Vec, Vec2)} | Rest]);
fold_clusters_sorted([{CLNo, Count, Vec}, {CLNo2, Count2, Vec2} | Rest])
  when CLNo =/= CLNo2 ->
    [{CLNo, Count, Vec} | fold_clusters_sorted([{CLNo2, Count2, Vec2} | Rest])].

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

%% Euclidian distance metric for now.
-spec euclidian_distance(kmeans_vector(A), kmeans_vector(A)) -> float().
euclidian_distance(Vec, Clus) ->
    Q = euclidian_distance(Vec, Clus, 0),
    math:sqrt(Q).

euclidian_distance([], [], Q) -> Q;
euclidian_distance([], [{_C2, V2} | R2], Q) ->
    R = (0 - V2) * (0 - V2),
    euclidian_distance([], R2, Q + R);
euclidian_distance([{_C1, V1} | R1], [], Q) ->
    R = (V1 - 0) * (V1 - 0),
    euclidian_distance(R1, [], Q + R);
euclidian_distance([{C1, V1} | R1], [{C2, V2} | R2], Q)
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

-spec create_random_cluster([A]) -> kmeans_vector(A).
create_random_cluster([]) -> [];
create_random_cluster([X | Dims]) ->
    R = crypto:rand_uniform(0, 1000*1000*1000) / (1000 * 1000 * 1000),
    [{X, R * 2 - 1} | create_random_cluster(Dims)].

create_clusters(0, _Dims) -> [];
create_clusters(N, Dims) when N > 0 ->
    [create_random_cluster(Dims) | create_clusters(N-1, Dims)].

initialize_clusters(RC, Bucket, N) ->
    Dimensions = find_dimensions(RC, Bucket),
    create_clusters(N, Dimensions).

kmeans_iterate(C, Bucket, Clusters, N) when length(Clusters) < N ->
    %% If nobody are sorted into a cluster, reset that cluster and try again
    %% TODO: Store the dimensions and reuse, this might end up being expensive.
    NewCls = initialize_clusters(C, Bucket, N - length(Clusters)),
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

-spec kmeans_difference([kmeans_vector(A)],
			[kmeans_vector(A)]) -> float().
kmeans_difference(Old, New) ->
    lists:sum([euclidian_distance(V1, V2) || {V1, V2} <- lists:zip(Old, New)]).

-spec kmeans_step(pid(), term(), [{integer(), kmeans_vector(A)}]) ->
			 [{integer(), kmeans_vector(A)}].
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

-spec step_post_process([{integer(), integer(), kmeans_vector(term())}]) ->
			       [{integer(), kmeans_vector(term())}].
step_post_process(NewClusters) ->
    NC = lists:keysort(1, NewClusters),
    F = fun(Count, Vec) ->
		[{D, V / Count} || {D, V} <- Vec]
	end,
    [F(Cnt, Vec) || {_, Cnt, Vec} <- NC].

find_dimensions(RC, Bucket) ->
    {ok, [{1, [Set]}]} =
	riakc_pb_socket:mapred_bucket(
	  RC, Bucket,
	  [{map,
	    {modfun, gain_kmeans, map_get_dimension_names},
	    undefined, false},
	   {reduce, {modfun, gain_kmeans, red_set_union},
	    undefined, true}]),
    lists:sort(sets:to_list(Set)).

%% ----------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

euclidian_dist_1_test() ->
    Vec1 = [{1, 5}],
    Vec2 = [],
    ?assertEqual(euclidian_distance(Vec1, Vec2), 5.0).

euclidian_dist_2_test() ->
    Vec1 = [],
    Vec2 = [{1, 1}, {2, 1}],
    Vec3 = [{3, 2}, {4, 2}],
    ?assertEqual(math:sqrt(2), euclidian_distance(Vec1, Vec2)),
    ?assertEqual(math:sqrt(2), euclidian_distance(Vec2, Vec1)),
    ?assertEqual(math:sqrt(1 + 1 + 2*2 + 2*2), euclidian_distance(Vec2, Vec3)),
    ?assertEqual(math:sqrt(1 + 1 + 2*2 + 2*2), euclidian_distance(Vec3, Vec2)).

create_cluster_1_test() ->
    N = 3,
    Cs = create_clusters(N, [1,2,3,4]),
    ?assertEqual(length(Cs), 3),
    ?assertEqual([length(X) || X <- Cs], [4,4,4]).

find_nearest_cluster_1_test() ->
    RandClusters = create_clusters(3, [1,2,3,4]),
    Vec = [{1, 1}, {2, -1}, {4, 0.5}],
    N = find_nearest_cluster(Vec, RandClusters ++ [Vec]),
    ?assertEqual(N, 3),
    N2 = find_nearest_cluster(Vec, [Vec | RandClusters]),
    ?assertEqual(N2, 0).

sum_vec_1_test() ->
    Vec1 = [],
    ?assertEqual(Vec1, sum_vec(Vec1, Vec1)),
    Vec2 = [{1,1}, {2, -1}],
    ?assertEqual(Vec2, sum_vec(Vec1, Vec2)),
    ?assertEqual(Vec2, sum_vec(Vec2, Vec1)),
    Vec3 = [{1,1}, {2, 1}],
    ?assertEqual([{1,2}, {2,0}], sum_vec(Vec2, Vec3)),
    ?assertEqual([{1,2}, {2,0}], sum_vec(Vec3, Vec2)),
    Vec4 = [{1,1}, {4,2}],
    ?assertEqual([{1,2}, {2, -1}, {4,2}], sum_vec(Vec2, Vec4)),
    ?assertEqual([{1,2}, {2, -1}, {4,2}], sum_vec(Vec4, Vec2)).

fold_clusters_sorted_1_test() ->
    ?assertEqual([], fold_clusters_sorted([])),
    C1 = {1, 1, [{1,2}]},
    ?assertEqual([C1], fold_clusters_sorted([C1])),
    C2 = {1, 3, [{2,3}]},
    ?assertEqual([{1, 4, [{1,2}, {2,3}]}], fold_clusters_sorted([C1, C2])),
    C3 = {2, 37, [{1,1},{2,4}]},
    CanonRes = [{1, 4, [{1,2}, {2,3}]}, C3],
    ?assertEqual(CanonRes, fold_clusters_sorted([C1, C2, C3])),
    RC = fold_clusters([C1, C3, C2]),
    ?assertEqual(CanonRes, RC).

-endif.
