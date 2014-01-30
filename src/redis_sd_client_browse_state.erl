%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pagodabox.com>
%%% @copyright 2014, Pagoda Box, Inc.
%%% @doc
%%%
%%% @end
%%% Created :  02 Sep 2013 by Andrew Bennett <andrew@pagodabox.com>
%%%-------------------------------------------------------------------
-module(redis_sd_client_browse_state).
-behaviour(gen_server).

-include("redis_sd_client.hrl").

%% API
-export([start_link/1, add/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	terminate/2, code_change/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @private
start_link(Browse=?REDIS_SD_BROWSE{ref=Ref}) ->
	gen_server:start_link({via, redis_sd_client, {state_pid, Ref}}, ?MODULE, Browse, []).

add(BrowseRef, Key, Record) ->
	gen_server:cast({via, redis_sd_client, {state_pid, BrowseRef}}, {add, Key, Record}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init(Browse=?REDIS_SD_BROWSE{}) ->
	Table = ets:new(?MODULE, [ordered_set, protected]),
	timer:send_interval(1000, tick),
	{ok, Browse?REDIS_SD_BROWSE{table=Table}}.

%% @private
handle_call(Request, From, Browse) ->
	error_logger:warning_msg(
		"** ~p ~p unhandled request from ~p in ~p/~p~n"
		"   Request was: ~p~n",
		[?MODULE, self(), From, handle_call, 3, Request]),
	{reply, ignore, Browse}.

%% @private
handle_cast({add, Key, Record=?REDIS_SD_DNS{ttl=0}}, Browse=?REDIS_SD_BROWSE{table=Table}) ->
	redis_sd_client_event:record_expire(Key, Record, Browse),
	true = ets:delete(Table, Key),
	{noreply, Browse};
handle_cast({add, Key, Record=?REDIS_SD_DNS{}}, Browse=?REDIS_SD_BROWSE{table=Table}) ->
	redis_sd_client_event:record_add(Key, Record, Browse),
	true = ets:insert(Table, {Key, Record}),
	{noreply, Browse};
handle_cast(Request, Browse) ->
	error_logger:warning_msg(
		"** ~p ~p unhandled request in ~p/~p~n"
		"   Request was: ~p~n",
		[?MODULE, self(), handle_cast, 2, Request]),
	{noreply, Browse}.

%% @private
handle_info(tick, Browse=?REDIS_SD_BROWSE{table=Table}) ->
	FoldFun = fun
		({Key, Record=?REDIS_SD_DNS{ttl=TTL0}}, Acc) ->
			case TTL0 - 1 of
				TTL when TTL =< 0 ->
					redis_sd_client_event:record_expire(Key, Record?REDIS_SD_DNS{ttl=0}, Browse),
					true = ets:delete(Table, Key),
					Acc;
				TTL ->
					true = ets:insert(Table, {Key, Record?REDIS_SD_DNS{ttl=TTL}}),
					Acc
			end
	end,
	true = ets:foldl(FoldFun, true, Table),
	{noreply, Browse};
handle_info(Info, Browse) ->
	error_logger:warning_msg(
		"** ~p ~p unhandled info in ~p/~p~n"
		"   Info was: ~p~n",
		[?MODULE, self(), handle_info, 2, Info]),
	{noreply, Browse}.

%% @private
terminate(_Reason, _Browse) ->
	ok.

%% @private
code_change(_OldVsn, Browse, _Extra) ->
	{ok, Browse}.
