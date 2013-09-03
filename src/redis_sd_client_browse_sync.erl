%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pagodabox.com>
%%% @copyright 2013, Pagoda Box, Inc.
%%% @doc
%%%
%%% @end
%%% Created :  31 Aug 2013 by Andrew Bennett <andrew@pagodabox.com>
%%%-------------------------------------------------------------------
-module(redis_sd_client_browse_sync).
-behaviour(gen_fsm).

-include("redis_sd_client.hrl").

%% API
-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
	terminate/3, code_change/4]).

-export([connect/2, authorize/2, read/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @private
start_link(Browse=#browse{}) ->
	gen_fsm:start_link(?MODULE, Browse, []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @private
init(Browse=#browse{}) ->
	{ok, connect, Browse, 0}.

%% @private
handle_event(_Event, _StateName, Browse) ->
	{stop, badmsg, Browse}.

%% @private
handle_sync_event(_Event, _From, _StateName, Browse) ->
	{stop, badmsg, Browse}.

%% @private
handle_info(Info, StateName, Browse=#browse{name=Name}) ->
	error_logger:error_msg(
		"** ~p ~p unhandled info in ~p/~p~n"
		"   Info was: ~p~n",
		[?MODULE, Name, handle_info, 3, Info]),
	{next_state, StateName, Browse, 0}.

%% @private
terminate(_Reason, _StateName, _Browse) ->
	ok.

%% @private
code_change(_OldVsn, StateName, Browse, _Extra) ->
	{ok, StateName, Browse}.

%%%===================================================================
%%% States
%%%===================================================================

%% @private
connect(timeout, Browse=#browse{redis_opts={Transport, Args}}) ->
	ConnectFun = case Transport of
		tcp ->
			connect;
		unix ->
			connect_unix
	end,
	case erlang:apply(hierdis_async, ConnectFun, Args) of
		{ok, Client} ->
			{next_state, authorize, Browse#browse{redis_cli=Client}, 0};
		{error, ClientConnectError} ->
			{stop, {error, ClientConnectError}, Browse}
	end.

%% @private
authorize(timeout, Browse=#browse{redis_auth=undefined}) ->
	{next_state, read, Browse, 0};
authorize(timeout, Browse=#browse{redis_auth=Password}) when Password =/= undefined ->
	case redis_auth(Password, Browse) of
		ok ->
			{next_state, read, Browse, 0};
		{error, Reason} ->
			{stop, {error, Reason}, Browse}
	end.

%% @private
read(timeout, Browse) ->
	case redis_keys(Browse) of
		{ok, Keys} ->
			case redis_keyvals(Keys, Browse) of
				{ok, KeyVals} ->
					% redis_sd_client_event:browse_sync(Vals, Browse),
					ok = redis_sd_client_reader:read(Browse, KeyVals),
					% ok = gen_server:cast(Browse#browse.reader, {sync, KeyVals}),
					{stop, normal, Browse};
				{error, ValReason} ->
					{stop, {error, ValReason}, Browse}
			end;
		{error, KeyReason} ->
			{stop, {error, KeyReason}, Browse}
	end.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
redis_auth(Password, #browse{redis_cli=Client, cmd_auth=AUTH}) ->
	Command = [AUTH, Password],
	try hierdis_async:command(Client, Command) of
		{ok, _} ->
			ok;
		{error, Reason} ->
			{error, Reason}
	catch
		Class:Reason ->
			error_logger:error_msg(
				"** ~p ~p terminating in ~p/~p~n"
				"   for the reason ~p:~p~n** Stacktrace: ~p~n~n",
				[?MODULE, self(), redis_auth, 2, Class, Reason, erlang:get_stacktrace()]),
			erlang:error(Reason)
	end.

%% @private
redis_keys(#browse{redis_cli=Client, redis_ns=Namespace, glob=Pattern, cmd_keys=KEYS}) ->
	Keys = [Namespace, "PTR:", Pattern],
	Command = [KEYS, Keys],
	try hierdis_async:command(Client, Command) of
		{ok, Result} ->
			{ok, Result};
		{error, Reason} ->
			{error, Reason}
	catch
		Class:Reason ->
			error_logger:error_msg(
				"** ~p ~p terminating in ~p/~p~n"
				"   for the reason ~p:~p~n** Stacktrace: ~p~n~n",
				[?MODULE, self(), redis_keys, 1, Class, Reason, erlang:get_stacktrace()]),
			erlang:error(Reason)
	end.

%% @private
redis_keyvals([], #browse{}) ->
	{ok, []};
redis_keyvals(Keys, #browse{redis_cli=Client, cmd_mget=MGET, cmd_ttl=TTL}) ->
	Transaction = [
		[MGET | Keys]
		| [[TTL, Key] || Key <- Keys]
	],
	try hierdis_async:transaction(Client, Transaction) of
		{ok, [Vals | TTLs]} ->
			{ok, zipkeys(Keys, Vals, TTLs, [])};
		{error, Reason} ->
			{error, Reason}
	catch
		Class:Reason ->
			error_logger:error_msg(
				"** ~p ~p terminating in ~p/~p~n"
				"   for the reason ~p:~p~n** Stacktrace: ~p~n~n",
				[?MODULE, self(), redis_keyvals, 2, Class, Reason, erlang:get_stacktrace()]),
			erlang:error(Reason)
	end.

%% @private
zipkeys([], [], [], Acc) ->
	lists:reverse(Acc);
zipkeys([Key | Keys], [Val | Vals], [TTL | TTLs], Acc) ->
	case catch inet_dns:decode(Val) of
		{ok, DNS} ->
			zipkeys(Keys, Vals, TTLs, [{Key, DNS, TTL} | Acc]);
		_ ->
			zipkeys(Keys, Vals, TTLs, Acc)
	end.
