%%%-------------------------------------------------------------------
%%% File    : reddit_rec.erl
%%% Author  : Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%% Description : Reddit Recommendation code
%%%
%%% Created : 26 Oct 2010 by Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%%-------------------------------------------------------------------
-module(reddit_rec).

%% API
-export([read_file/1, kmeans/2, grab_subreddits/3, red_set_union/2]).

-define(BUCKET, <<"affinities">>).

%%====================================================================
%% API
%%====================================================================
new_term(Bucket, Key, Value) ->
    riakc_obj:new(Bucket, Key, term_to_binary(Value, [compressed])).

read_file(FName) ->
    {ok, RC} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    {ok, IO} = file:open(FName, [read, raw, {read_ahead, 8192}]),
    process_lines(RC, "foo", [{fake, {0,0}}], IO),
    riakc_pb_socket:delete(RC, ?BUCKET, <<"foo">>),
    ok.

skew([]) -> [];
skew([{_SubReddit, {Ups, Downs}} | Rest]) when Ups + Downs < 3 -> skew(Rest);
skew([{SubReddit, {Ups, Downs}} | Rest]) ->
    Affinity = Ups / (Ups + Downs),
    Skew = Affinity * 2 - 1,
    [{SubReddit, Skew} | skew(Rest)].

store(RC, Key, Val) ->
    Obj = new_term(?BUCKET, binarize(Key), Val),
    riakc_pb_socket:put(RC, Obj, [{w,1}]).

update_affinities(Vote, {Ups, Downs}) when Vote == 1 -> {Ups+1, Downs};
update_affinities(Vote, {Ups, Downs}) when Vote == -1 -> {Ups, Downs+1}.

process_lines(RC, LastUser, Rs = [{LastSubReddit, UpsDowns} | Reddits], IO) ->
    case file:read_line(IO) of
	eof ->
	    ok = store(RC, LastUser, lists:reverse(skew(Rs))),
	    done;
	{ok, L} ->
	    {Key, Subreddit, Vote} = parse_line(L),
	    case Key == LastUser of
		true ->
		    case Subreddit == LastSubReddit of
			true ->
			    process_lines(RC, LastUser,
					  [{LastSubReddit,
					    update_affinities(Vote, UpsDowns)} | Reddits],
					  IO);
			false ->
			    process_lines(RC, LastUser,
					  [{Subreddit,
					    update_affinities(Vote, {0,0})} | Rs],
					  IO)
		    end;
		false ->
		    ok = store(RC, LastUser, lists:reverse(skew(Rs))),
		    process_lines(RC, Key,
				  [{Subreddit,
				    update_affinities(Vote, {0,0})}],
				  IO)
	    end
    end.

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

kmeans(Bucket, N) ->
    Clusters = initialize_clusters(Bucket, N),
    kmeans_iterate(Bucket, Clusters, N).

kmeans_iterate(Bucket, Clusters, N) when length(Clusters) < N ->
    NewCls = create_clusters(Bucket, N - length(Clusters)),
    kmeans_iterate(Bucket, NewCls ++ Clusters, N);
kmeans_iterate(_Bucket, _Clusters, _N) ->
    todo.

grab_subreddits(Obj, _, _) ->
    L = binary_to_term(riak_object:get_value(Obj)),
    SubReddits = [SR || {SR, _} <- L],
    [sets:from_list(SubReddits)].

red_set_union(List, _) ->
    Union = sets:union(List),
    [Union].

find_dimensions(Bucket) ->
    {ok, RC} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    {ok, [{1, [Set]}]} =
	riakc_pb_socket:mapred_bucket(
	  RC, Bucket,
	  [{map, {modfun, reddit_rec, grab_subreddits}, undefined, false},
	   {reduce, {modfun, reddit_rec, red_set_union},
	    undefined, true}]),
    lists:sort(sets:to_list(Set)).

parse_line(L) ->
    [Key, SubReddit, VoteNS] = string:tokens(L, "\t "),
    Vote = string:strip(VoteNS, both, $\n),
    {Key, SubReddit, list_to_integer(Vote)}.

binarize(K) ->
    list_to_binary(K).

%%====================================================================
%% Internal functions
%%====================================================================
