%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pagodabox.com>
%%% @copyright 2014, Pagoda Box, Inc.
%%% @doc
%%%
%%% @end
%%% Created :  31 Aug 2013 by Andrew Bennett <andrew@pagodabox.com>
%%%-------------------------------------------------------------------
-module(redis_sd_client_browse_sync).

-include("redis_sd_client.hrl").

%% API
-export([start_link/1]).

%% proc_lib callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @private
start_link(Browse=?REDIS_SD_BROWSE{}) ->
	proc_lib:start_link(?MODULE, init, [Browse]).

%%%===================================================================
%%% proc_lib callbacks
%%%===================================================================

%% @private
init(Browse=?REDIS_SD_BROWSE{}) ->
	ok = proc_lib:init_ack({ok, self()}),
	connect(Browse).

%%%-------------------------------------------------------------------
%%% States
%%%-------------------------------------------------------------------

%% @private
connect(Browse=?REDIS_SD_BROWSE{redis_opts={Transport, Args}}) ->
	ConnectFun = case Transport of
		tcp ->
			connect;
		unix ->
			connect_unix
	end,
	case erlang:apply(hierdis_async, ConnectFun, Args) of
		{ok, Client} ->
			authorize(Browse?REDIS_SD_BROWSE{redis_cli=Client});
		{error, _ConnectError} ->
			terminate(normal, Browse)
	end.

%% @private
authorize(Browse=?REDIS_SD_BROWSE{redis_auth=undefined}) ->
	read(Browse);
authorize(Browse=?REDIS_SD_BROWSE{redis_auth=Password}) when Password =/= undefined ->
	case redis_auth(Password, Browse) of
		ok ->
			read(Browse);
		{error, _AuthError} ->
			terminate(normal, Browse)
	end.

%% @private
read(Browse=?REDIS_SD_BROWSE{ref=BrowseRef}) ->
	case redis_keys(Browse) of
		{ok, Keys} ->
			case redis_keyvals(Keys, Browse) of
				{ok, KeyVals} ->
					_ = [begin
						redis_sd_client_browse_state:add(BrowseRef, Key, Record)
					end || {Key, Record} <- KeyVals],
					terminate(normal, Browse);
				{error, _KeyValsReason} ->
					terminate(normal, Browse)
			end;
		{error, _KeysReason} ->
			terminate(normal, Browse)
	end.

%% @private
terminate(Reason, ?REDIS_SD_BROWSE{redis_cli=Client}) ->
	case Client of
		undefined ->
			ok;
		_ ->
			catch hierdis_async:close(Client)
	end,
	exit(Reason).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
redis_auth(Password, ?REDIS_SD_BROWSE{redis_cli=Client, cmd_auth=AUTH}) ->
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
redis_keys(?REDIS_SD_BROWSE{redis_cli=Client, channels=Channels, cmd_keys=KEYS}) ->
	Transaction = [[KEYS, Channel] || Channel <- Channels],
	try hierdis_async:transaction(Client, Transaction) of
		{ok, [Keys]} ->
			{ok, Keys};
		{ok, [[], Keys]} ->
			{ok, Keys};
		{ok, [Keys, []]} ->
			{ok, Keys};
		{ok, [KeysA, KeysB]} ->
			{ok, gb_sets:to_list(gb_sets:union(gb_sets:from_list(KeysA), gb_sets:from_list(KeysB)))};
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
redis_keyvals([], ?REDIS_SD_BROWSE{}) ->
	{ok, []};
redis_keyvals(Keys, ?REDIS_SD_BROWSE{redis_cli=Client, cmd_mget=MGET, cmd_ttl=TTL}) ->
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
	case catch redis_sd_dns:from_binary(Val) of
		Record=?REDIS_SD_DNS{} ->
			zipkeys(Keys, Vals, TTLs, [{Key, Record?REDIS_SD_DNS{ttl=TTL}} | Acc]);
		_ ->
			zipkeys(Keys, Vals, TTLs, Acc)
	end.
