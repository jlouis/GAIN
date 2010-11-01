%%%-------------------------------------------------------------------
%%% File    : reddit_rec.erl
%%% Author  : Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%% Description : Reddit Recommendation code
%%%
%%% Created : 26 Oct 2010 by Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%%-------------------------------------------------------------------
-module(reddit_rec).

%% API
-export([read_file/1, kmeans/0, keys/0, dimensions/0]).

-define(BUCKET, <<"affinities">>).

%%====================================================================
%% API
%%====================================================================
new_term(Bucket, Key, Value) ->
    riakc_obj:new(Bucket, Key, term_to_binary(Value, [compressed])).

read_file(FName) ->
    {ok, RC} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    {ok, IO} = file:open(FName, [read, raw, {read_ahead, 8192}]),
    N = process_lines(RC, "foo", [{fake, {0,0}}], IO, 0),
    riakc_pb_socket:delete(RC, ?BUCKET, <<"foo">>),
    {ok, N}.

dimensions() ->
    {ok, RC} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    gain_kmeans:find_dimensions(RC, ?BUCKET).

keys() ->
    {ok, RC} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    riakc_pb_socket:list_keys(RC, ?BUCKET).

kmeans() ->
    {ok, RC} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    gain_kmeans:kmeans(RC, ?BUCKET, 5).

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

process_lines(RC, LastUser, Rs = [{LastSubReddit, UpsDowns} | Reddits], IO, N) ->
    case file:read_line(IO) of
	eof ->
	    ok = store(RC, LastUser, lists:reverse(skew(Rs))),
	    N;
	{ok, L} ->
	    {Key, Subreddit, Vote} = parse_line(L),
	    case Key == LastUser of
		true ->
		    case Subreddit == LastSubReddit of
			true ->
			    process_lines(RC, LastUser,
					  [{LastSubReddit,
					    update_affinities(Vote, UpsDowns)} | Reddits],
					  IO,
					  N+1);
			false ->
			    process_lines(RC, LastUser,
					  [{Subreddit,
					    update_affinities(Vote, {0,0})} | Rs],
					  IO,
					  N+1)
		    end;
		false ->
		    ok = store(RC, LastUser, lists:reverse(skew(Rs))),
		    process_lines(RC, Key,
				  [{Subreddit,
				    update_affinities(Vote, {0,0})}],
				  IO,
				  N+1)
	    end
    end.



parse_line(L) ->
    [Key, SubReddit, VoteNS] = string:tokens(L, "\t "),
    Vote = string:strip(VoteNS, both, $\n),
    {Key, SubReddit, list_to_integer(Vote)}.

binarize(K) ->
    list_to_binary(K).

%%====================================================================
%% Internal functions
%%====================================================================
