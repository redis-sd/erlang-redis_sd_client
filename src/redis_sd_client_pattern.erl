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
-export([get/2, hostname/1, service/1, type/1, domain/1, greedy/1, refresh/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Update and return the list of keys of the Browse.
get([], Browse) ->
	{ok, [], Browse};
get(List, Browse) ->
	g(List, Browse, []).

hostname(Browse=#browse{hostname=undefined}) ->
	H = <<>>,
	{ok, H, Browse#browse{h_glob=H}};
hostname(Browse=#browse{hostname=Hostname}) ->
	H = iolist_to_binary([$., glob(Hostname)]),
	{ok, H, Browse#browse{h_glob=H}}.

service(Browse=#browse{service=undefined}) ->
	S = <<"_*">>,
	{ok, S, Browse#browse{s_glob=S}};
service(Browse=#browse{service=Service}) ->
	S = iolist_to_binary([$_, glob(Service)]),
	{ok, S, Browse#browse{s_glob=S}}.

type(Browse=#browse{type=undefined}) ->
	T = <<"_*.">>,
	{ok, T, Browse#browse{t_glob=T}};
type(Browse=#browse{type=Type}) ->
	T = iolist_to_binary([$_, glob(Type), $.]),
	{ok, T, Browse#browse{t_glob=T}}.

domain(Browse=#browse{domain=undefined}) ->
	D = <<"*.">>,
	{ok, D, Browse#browse{d_glob=D}};
domain(Browse=#browse{domain=Domain}) ->
	D = iolist_to_binary([redis_sd:nsreverse(glob(Domain)), $.]),
	{ok, D, Browse#browse{d_glob=D}}.

greedy(Browse=#browse{greedy=true}) ->
	G = <<"*">>,
	{ok, G, Browse#browse{g_glob=G}};
greedy(Browse) ->
	G = <<>>,
	{ok, G, Browse#browse{g_glob=G}}.

%% @doc Update and return the pattern for the Browse.
refresh(Browse=#browse{}) ->
	case get([domain, type, service, hostname, greedy], Browse) of
		{ok, GlobList, Browse2} ->
			Pattern = iolist_to_binary(GlobList),
			{ok, Pattern, Browse2#browse{glob=Pattern}};
		Error ->
			Error
	end.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
g([], Browse, Acc) ->
	{ok, lists:reverse(Acc), Browse};
g([Key | Keys], Browse, Acc) when Key =/= glob ->
	case ?MODULE:Key(Browse) of
		{ok, Val, Browse2} ->
			g(Keys, Browse2, [Val | Acc]);
		Error ->
			Error
	end.

%% @private
glob('*') ->
	<< $* >>;
glob('?') ->
	<< $? >>;
glob(Atom) when is_atom(Atom) ->
	atom_to_binary(Atom, utf8);
glob(Binary) when is_binary(Binary) ->
	redis_sd:urlencode(Binary);
glob(List) when is_list(List) ->
	case catch iolist_to_binary(List) of
		Binary when is_binary(Binary) ->
			Binary;
		_ ->
			glob(List, [])
	end.

%% @private
glob([], Glob) ->
	iolist_to_binary(lists:reverse(Glob));
glob([Token | Tokens], Glob) ->
	glob(Tokens, [glob(Token) | Glob]).
