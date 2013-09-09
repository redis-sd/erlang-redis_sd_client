%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pagodabox.com>
%%% @copyright 2013, Pagoda Box, Inc.
%%% @doc
%%%
%%% @end
%%% Created :  30 Aug 2013 by Andrew Bennett <andrew@pagodabox.com>
%%%-------------------------------------------------------------------
-module(redis_sd_client_pattern).

-include("redis_sd_client.hrl").

%% API
-export([refresh/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Update and return the pattern for the Browse.
refresh(Browse=#browse{domain=D, type=T, service=S, instance=I, greedy=G}) ->
	Patterns = patterns([D, T, S, I], G, 0, []),
	{ok, Patterns, Browse}.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
patterns([undefined | Rest], G, C, P) ->
	patterns(Rest, G, C+1, P);
patterns([Label | Rest], G, 0, P) ->
	patterns(Rest, G, 1, [redis_sd_ns:reverse(glob(Label, false)) | P]);
patterns([Label | Rest], G, 3, P) ->
	patterns(Rest, G, 4, [glob(Label, true) | P]);
patterns([Label | Rest], G, C, P) when C == 1 orelse C == 2 ->
	patterns(Rest, G, C+1, [[$_, glob(Label, false)] | fill(C, P)]);
patterns([Label | Rest], G, C, P) ->
	patterns(Rest, G, C+1, [glob(Label, false) | fill(C, P)]);
patterns([], _G, _C, []) ->
	[<<"*">>];
patterns([], _G, C, P) when length(P) == C ->
	[glob_join(P, [])];
patterns([], true, _C, P) ->
	[glob_join(P, []), glob_join([<<"*">> | P], [])];
patterns([], _G, _C, P) ->
	[glob_join(P, [])].

%% @private
fill({C, C}, P) ->
	P;
fill({C, _N}, P) when length(P) == C ->
	P;
fill({C, N}, P) when N == 1 orelse N == 2 ->
	fill({C, N+1}, [<<"_*">> | P]);
fill({C, N}, P) ->
	fill({C, N+1}, [<<"*">> | P]);
fill(C, P) ->
	fill({C, 0}, P).

%% @private
glob_join([], Glob) ->
	iolist_to_binary(Glob);
glob_join([Token | Tokens], []) ->
	glob_join(Tokens, [Token]);
glob_join([Token | Tokens], Glob) ->
	glob_join(Tokens, [[Token, $.] | Glob]).

%% @private
glob('*', _) ->
	<< $* >>;
glob('?', _) ->
	<< $? >>;
glob(Atom, Encode) when is_atom(Atom) ->
	glob(atom_to_binary(Atom, utf8), Encode);
glob(Binary, true) when is_binary(Binary) ->
	redis_sd:urlencode(Binary);
glob(Binary, false) when is_binary(Binary) ->
	Binary;
glob(List, Encode) when is_list(List) ->
	case catch iolist_to_binary(List) of
		Binary when is_binary(Binary) ->
			glob(Binary, Encode);
		_ ->
			glob(List, Encode, [])
	end.

%% @private
glob([], Encode, Glob) ->
	glob(iolist_to_binary(lists:reverse(Glob)), Encode);
glob([Token | Tokens], Encode, Glob) ->
	glob(Tokens, Encode, [glob(Token, Encode) | Glob]).
