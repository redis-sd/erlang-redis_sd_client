%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pagodabox.com>
%%% @copyright 2014, Pagoda Box, Inc.
%%% @doc
%%%
%%% @end
%%% Created :  30 Aug 2013 by Andrew Bennett <andrew@pagodabox.com>
%%%-------------------------------------------------------------------
-module(redis_sd_client_browse).
-behaviour(gen_server).

-include("redis_sd_client.hrl").

%% API
-export([start_link/1, enable/1, disable/1, graceful_shutdown/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	terminate/2, code_change/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @private
start_link(Browse=?REDIS_SD_BROWSE{ref=Ref}) ->
	gen_server:start_link({via, redis_sd_client, {pid, Ref}}, ?MODULE, Browse, []).

%% @doc Enable announcing of the service.
enable(Ref) ->
	gen_server:cast({via, redis_sd_client, {pid, Ref}}, enable).

%% @doc Disable announcing of the service.
disable(Ref) ->
	gen_server:cast({via, redis_sd_client, {pid, Ref}}, disable).

%% @doc Gracefully shutdown the service.
graceful_shutdown(Ref) ->
	gen_server:call({via, redis_sd_client, {pid, Ref}}, graceful_shutdown).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init(Browse=?REDIS_SD_BROWSE{enabled=Enabled, min_wait=MinWait, max_wait=MaxWait}) ->
	case redis_sd_client_pattern:refresh(Browse) of
		{ok, Browse2} ->
			true = redis_sd_client:set_enabled(Browse2?REDIS_SD_BROWSE.ref, Enabled),
			redis_sd_client_event:browse_init(Browse2),
			case Enabled of
				false ->
					redis_sd_client_event:browse_disable(Browse2);
				true ->
					redis_sd_client_event:browse_enable(Browse2)
			end,
			Backoff = backoff:init(timer:seconds(MinWait), timer:seconds(MaxWait), self(), connect),
			BRef = start_connect(initial),
			{ok, Browse2?REDIS_SD_BROWSE{backoff=Backoff, bref=BRef}};
		RefreshError ->
			{stop, RefreshError}
	end.

%% @private
handle_call(graceful_shutdown, _From, Browse) ->
	{stop, normal, ok, Browse};
handle_call(Request, From, Browse) ->
	error_logger:warning_msg(
		"** ~p ~p unhandled request from ~p in ~p/~p~n"
		"   Request was: ~p~n",
		[?MODULE, self(), From, handle_call, 3, Request]),
	{reply, ignore, Browse}.

%% @private
handle_cast(enable, Browse=?REDIS_SD_BROWSE{enabled=false, ref=Ref, redis_sub=Subscriber}) ->
	true = redis_sd_client:set_enabled(Ref, true),
	Browse2 = case Subscriber of
		undefined ->
			Browse?REDIS_SD_BROWSE{enabled=true};
		_ ->
			start_sync(Browse?REDIS_SD_BROWSE{enabled=true})
	end,
	{noreply, Browse2};
handle_cast(enable, Browse=?REDIS_SD_BROWSE{enabled=true}) ->
	{noreply, Browse};
handle_cast(disable, Browse=?REDIS_SD_BROWSE{enabled=true, ref=Ref}) ->
	true = redis_sd_client:set_enabled(Ref, false),
	{noreply, Browse?REDIS_SD_BROWSE{enabled=false}};
handle_cast(disable, Browse=?REDIS_SD_BROWSE{enabled=false}) ->
	{noreply, Browse};
handle_cast(Request, Browse) ->
	error_logger:warning_msg(
		"** ~p ~p unhandled request in ~p/~p~n"
		"   Request was: ~p~n",
		[?MODULE, self(), handle_cast, 2, Request]),
	{noreply, Browse}.

%% @private
handle_info({timeout, BRef, connect}, Browse=?REDIS_SD_BROWSE{bref=BRef}) ->
	connect(Browse?REDIS_SD_BROWSE{bref=undefined});
handle_info({redis_message, Subscriber, [<<"psubscribe">>, Channel | _]}, Browse=?REDIS_SD_BROWSE{enabled=true, redis_sub=Subscriber}) ->
	redis_sd_client_event:browse_subscribe(Channel, Browse),
	Browse2 = start_sync(Browse),
	{noreply, Browse2};
handle_info({redis_message, Subscriber, [<<"psubscribe">>, Channel | _]}, Browse=?REDIS_SD_BROWSE{redis_sub=Subscriber}) ->
	redis_sd_client_event:browse_subscribe(Channel, Browse),
	{noreply, Browse};
handle_info({redis_message, Subscriber, [<<"pmessage">>, _Channel, Key, Message]}, Browse=?REDIS_SD_BROWSE{enabled=true, redis_sub=Subscriber}) ->
	case catch redis_sd_dns:from_binary(Message) of
		Record=?REDIS_SD_DNS{} ->
			ok = redis_sd_client_browse_state:add(Browse?REDIS_SD_BROWSE.ref, Key, Record),
			{noreply, Browse};
		_ ->
			{noreply, Browse}
	end;
handle_info({redis_message, Subscriber, [<<"pmessage">>, _Channel, _Key, _Message]}, Browse=?REDIS_SD_BROWSE{redis_sub=Subscriber}) ->
	{noreply, Browse};
handle_info({redis_error, Subscriber, {Error, Reason}}, Browse=?REDIS_SD_BROWSE{redis_sub=Subscriber, ref=Ref}) ->
	error_logger:warning_msg(
		"** ~p ~p received redis_error in ~p/~p~n"
		"   for the reason ~p:~p~n",
		[?MODULE, Ref, handle_info, 3, Error, Reason]),
	{noreply, Browse};
handle_info({redis_closed, Subscriber}, Browse=?REDIS_SD_BROWSE{redis_sub=Subscriber}) ->
	ok = hierdis_async:close(Subscriber),
	connect(Browse?REDIS_SD_BROWSE{redis_sub=undefined});
handle_info({'DOWN', SyncRef, process, SyncPid, _}, Browse=?REDIS_SD_BROWSE{sync={SyncPid, SyncRef}}) ->
	{noreply, Browse?REDIS_SD_BROWSE{sync=undefined}};
handle_info(Info, Browse=?REDIS_SD_BROWSE{ref=Ref}) ->
	error_logger:error_msg(
		"** ~p ~p unhandled info in ~p/~p~n"
		"   Info was: ~p~n",
		[?MODULE, Ref, handle_info, 3, Info]),
	{noreply, Browse}.

%% @private
terminate(Reason, Browse=?REDIS_SD_BROWSE{redis_sub=Subscriber}) ->
	case Subscriber of
		undefined ->
			ok;
		_ ->
			catch hierdis_async:close(Subscriber)
	end,
	redis_sd_client_event:browse_terminate(Reason, Browse?REDIS_SD_BROWSE{redis_sub=undefined}),
	ok.

%% @private
code_change(_OldVsn, Browse, _Extra) ->
	{ok, Browse}.

%%%===================================================================
%%% States
%%%===================================================================

%% @private
connect(Browse=?REDIS_SD_BROWSE{redis_opts={Transport, Args}, backoff=Backoff}) ->
	ConnectFun = case Transport of
		tcp ->
			connect;
		unix ->
			connect_unix
	end,
	case erlang:apply(hierdis_async, ConnectFun, Args) of
		{ok, Subscriber} ->
			authorize(Browse?REDIS_SD_BROWSE{redis_sub=Subscriber});
		{error, _ConnectError} ->
			BRef = start_connect(Browse),
			{_Delay, Backoff2} = backoff:fail(Backoff),
			{noreply, Browse?REDIS_SD_BROWSE{backoff=Backoff2, bref=BRef}}
	end.

%% @private
authorize(Browse=?REDIS_SD_BROWSE{redis_auth=undefined}) ->
	subscribe(Browse);
authorize(Browse=?REDIS_SD_BROWSE{redis_auth=Password, backoff=Backoff}) when Password =/= undefined ->
	case redis_auth(Password, Browse) of
		ok ->
			subscribe(Browse);
		{error, AuthError} ->
			error_logger:warning_msg("[~p] Redis auth error: ~p", [AuthError]),
			BRef = start_connect(Browse),
			{_Delay, Backoff2} = backoff:fail(Backoff),
			{noreply, Browse?REDIS_SD_BROWSE{backoff=Backoff2, bref=BRef}}
	end.

%% @private
subscribe(Browse=?REDIS_SD_BROWSE{channels=Channels, backoff=Backoff}) ->
	case redis_psubscribe(Channels, Browse) of
		ok ->
			redis_sd_client_event:browse_connect(Browse),
			{_Start, Backoff2} = backoff:succeed(Backoff),
			{noreply, Browse?REDIS_SD_BROWSE{backoff=Backoff2}};
		{error, SubscribeError} ->
			error_logger:warning_msg("[~p] Redis subscribe error: ~p", [?MODULE, SubscribeError]),
			BRef = start_connect(Browse),
			{_Delay, Backoff2} = backoff:fail(Backoff),
			{noreply, Browse?REDIS_SD_BROWSE{backoff=Backoff2, bref=BRef}}
	end.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
redis_auth(Password, ?REDIS_SD_BROWSE{redis_sub=Subscriber, cmd_auth=AUTH}) ->
	Command = [AUTH, Password],
	try hierdis_async:command(Subscriber, Command) of
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
redis_psubscribe(Channels, ?REDIS_SD_BROWSE{redis_sub=Subscriber, cmd_psubscribe=PSUBSCRIBE}) ->
	Command = [PSUBSCRIBE | Channels],
	try hierdis_async:append_command(Subscriber, Command) of
		{ok, _} ->
			ok;
		{error, Reason} ->
			{error, Reason}
	catch
		Class:Reason ->
			error_logger:error_msg(
				"** ~p ~p terminating in ~p/~p~n"
				"   for the reason ~p:~p~n** Stacktrace: ~p~n~n",
				[?MODULE, self(), redis_psubscribe, 2, Class, Reason, erlang:get_stacktrace()]),
			erlang:error(Reason)
	end.

%% @private
start_connect(initial) ->
	start_connect(0);
start_connect(Browse=?REDIS_SD_BROWSE{backoff=Backoff}) ->
	ok = stop_connect(Browse),
	Result = backoff:fire(Backoff),
	Result;
start_connect(N) when is_integer(N) ->
	erlang:start_timer(N, self(), connect).

%% @private
stop_connect(?REDIS_SD_BROWSE{bref=undefined}) ->
	ok;
stop_connect(?REDIS_SD_BROWSE{bref=BRef}) when is_reference(BRef) ->
	catch erlang:cancel_timer(BRef),
	ok.

%% @private
start_sync(Browse=?REDIS_SD_BROWSE{sync=undefined}) ->
	{ok, SyncPid} = redis_sd_client_browse_sup:start_sync(Browse),
	SyncRef = erlang:monitor(process, SyncPid),
	Browse?REDIS_SD_BROWSE{sync={SyncPid, SyncRef}};
start_sync(Browse=?REDIS_SD_BROWSE{}) ->
	Browse.
