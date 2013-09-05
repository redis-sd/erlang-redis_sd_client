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
-module(redis_sd_client_browse_reader).
-behaviour(gen_fsm).

-include("redis_sd_client.hrl").

%% API
-export([start_link/1, graceful_shutdown/1]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
	terminate/3, code_change/4]).

-export([connect/2, authorize/2, subscribe/2, wait/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @private
start_link(Browse=#browse{reader=ReaderName}) ->
	gen_fsm:start_link({local, ReaderName}, ?MODULE, Browse, []).

%% @doc Gracefully shutdown the browse.
graceful_shutdown(Name) ->
	gen_fsm:sync_send_all_state_event(Name, graceful_shutdown).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @private
init(Browse=#browse{min_wait=MinWait, max_wait=MaxWait}) ->
	redis_sd_client_event:browse_init(Browse),
	Backoff = backoff:init(timer:seconds(MinWait), timer:seconds(MaxWait), self(), connect),
	BRef = start_connect(initial),
	{ok, connect, Browse#browse{backoff=Backoff, bref=BRef}}.

%% @private
handle_event(_Event, _StateName, Browse) ->
	{stop, badmsg, Browse}.

%% @private
handle_sync_event(graceful_shutdown, _From, _StateName, Browse) ->
	{stop, normal, ok, Browse};
handle_sync_event(_Event, _From, _StateName, Browse) ->
	{stop, badmsg, Browse}.

%% @private
handle_info({timeout, BRef, connect}, _StateName, Browse=#browse{bref=BRef}) ->
	connect(timeout, Browse#browse{bref=undefined});
handle_info({timeout, ARef, authorize}, _StateName, Browse=#browse{aref=ARef}) ->
	authorize(timeout, Browse#browse{aref=undefined});
handle_info({timeout, SRef, subscribe}, subscribe, Browse=#browse{sref=SRef}) ->
	subscribe(timeout, Browse#browse{sref=undefined});
handle_info({redis_message, Subscriber, [<<"psubscribe">>, Channel | _]}, StateName, Browse=#browse{redis_sub=Subscriber, sync=undefined}) ->
	redis_sd_client_event:browse_subscribe(Channel, Browse),
	{ok, Pid} = redis_sd_client_browse_sup:start_sync(Browse),
	Ref = erlang:monitor(process, Pid),
	{next_state, StateName, Browse#browse{sync={Pid, Ref}}};
handle_info({redis_message, Subscriber, [<<"psubscribe">>, Channel | _]}, StateName, Browse=#browse{redis_sub=Subscriber}) ->
	redis_sd_client_event:browse_subscribe(Channel, Browse),
	{next_state, StateName, Browse};
handle_info({redis_message, Subscriber, [<<"pmessage">>, _Channel, Key, Message]}, StateName, Browse=#browse{redis_sub=Subscriber}) ->
	case catch inet_dns:decode(Message) of
		{ok, Val} ->
			ok = redis_sd_client_browse:parse(Browse, [{Key, Val, undefined}]),
			{next_state, StateName, Browse};
		_ ->
			{next_state, StateName, Browse}
	end;
handle_info({redis_error, Subscriber, {Error, Reason}}, StateName, Browse=#browse{redis_sub=Subscriber, name=Name}) ->
	error_logger:warning_msg(
		"** ~p ~p received redis_error in ~p/~p~n"
		"   for the reason ~p:~p~n",
		[?MODULE, Name, handle_info, 3, Error, Reason]),
	{next_state, StateName, Browse};
handle_info({redis_closed, Subscriber}, _StateName, Browse=#browse{redis_sub=Subscriber}) ->
	ok = stop_subscribe(Browse),
	ok = hierdis_async:close(Subscriber),
	connect(timeout, Browse#browse{redis_sub=undefined, sref=undefined});
handle_info({'DOWN', Ref, process, Pid, _}, StateName, Browse=#browse{sync={Pid, Ref}}) ->
	{next_state, StateName, Browse#browse{sync=undefined}};
handle_info(Info, StateName, Browse=#browse{name=Name}) ->
	error_logger:error_msg(
		"** ~p ~p unhandled info in ~p/~p~n"
		"   Info was: ~p~n",
		[?MODULE, Name, handle_info, 3, Info]),
	{next_state, StateName, Browse, 0}.

%% @private
terminate(Reason, _StateName, Browse=#browse{redis_sub=Subscriber}) ->
	catch hierdis_async:close(Subscriber),
	redis_sd_client_event:browse_terminate(Reason, Browse),
	ok.

%% @private
code_change(_OldVsn, StateName, Browse, _Extra) ->
	{ok, StateName, Browse}.

%%%===================================================================
%%% States
%%%===================================================================

%% @private
connect(timeout, Browse=#browse{redis_opts={Transport, Args}, backoff=Backoff}) ->
	redis_sd_client_event:browse_connect(Browse),
	ConnectFun = case Transport of
		tcp ->
			connect;
		unix ->
			connect_unix
	end,
	case erlang:apply(hierdis_async, ConnectFun, Args) of
		{ok, Subscriber} ->
			{_Start, Backoff2} = backoff:succeed(Backoff),
			ARef = start_authorize(initial),
			{next_state, subscribe, Browse#browse{redis_sub=Subscriber, backoff=Backoff2, aref=ARef}};
		{error, _SubscriberConnectError} ->
			BRef = start_connect(Browse),
			{_Delay, Backoff2} = backoff:fail(Backoff),
			{next_state, connect, Browse#browse{backoff=Backoff2, bref=BRef}}
	end;
connect(timeout, Browse=#browse{redis_opts=BadRedisOpts}) ->
	{stop, {error, {bad_redis_opts, BadRedisOpts}}, Browse}.

%% @private
authorize(timeout, Browse=#browse{redis_auth=undefined}) ->
	SRef = start_subscribe(initial),
	{next_state, subscribe, Browse#browse{sref=SRef}};
authorize(timeout, Browse=#browse{redis_auth=Password}) when Password =/= undefined ->
	case redis_auth(Password, Browse) of
		ok ->
			SRef = start_subscribe(initial),
			{next_state, subscribe, Browse#browse{sref=SRef}};
		{error, Reason} ->
			{stop, {error, Reason}, Browse}
	end.

%% @private
subscribe(timeout, Browse=#browse{}) ->
	case redis_sd_client_pattern:refresh(Browse) of
		{ok, Patterns, Browse2} ->
			case redis_psubscribe(Patterns, Browse2) of
				{ok, Channels} ->
					{next_state, wait, Browse2#browse{channels=Channels}};
				{error, Reason} ->
					{stop, {error, Reason}, Browse2}
			end;
		RefreshError ->
			{stop, RefreshError, Browse}
	end.

%% @private
wait(_Event, Browse) ->
	{next_state, wait, Browse}.

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
redis_psubscribe(Patterns, #browse{redis_sub=Subscriber, redis_ns=Namespace, cmd_psubscribe=PSUBSCRIBE}) ->
	Channels = [iolist_to_binary([Namespace, "PTR:", Pattern]) || Pattern <- Patterns],
	Command = [PSUBSCRIBE | Channels],
	try hierdis_async:append_command(Subscriber, Command) of
		{ok, _} ->
			{ok, Channels};
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
start_authorize(initial) ->
	start_authorize(0);
start_authorize(Browse=#browse{}) ->
	ok = stop_authorize(Browse),
	start_authorize(0);
start_authorize(N) when is_integer(N) ->
	erlang:start_timer(N, self(), authorize).

%% @private
stop_authorize(#browse{aref=undefined}) ->
	ok;
stop_authorize(#browse{aref=ARef}) when is_reference(ARef) ->
	catch erlang:cancel_timer(ARef),
	ok.

%% @private
start_connect(initial) ->
	start_connect(0);
start_connect(Browse=#browse{backoff=Backoff}) ->
	ok = stop_connect(Browse),
	Result = backoff:fire(Backoff),
	Result;
start_connect(N) when is_integer(N) ->
	erlang:start_timer(N, self(), connect).

%% @private
stop_connect(#browse{bref=undefined}) ->
	ok;
stop_connect(#browse{bref=BRef}) when is_reference(BRef) ->
	catch erlang:cancel_timer(BRef),
	ok.

%% @private
start_subscribe(initial) ->
	start_subscribe(0);
start_subscribe(Browse=#browse{}) ->
	ok = stop_subscribe(Browse),
	start_subscribe(0);
start_subscribe(N) when is_integer(N) ->
	erlang:start_timer(N, self(), subscribe).

%% @private
stop_subscribe(#browse{sref=undefined}) ->
	ok;
stop_subscribe(#browse{sref=SRef}) when is_reference(SRef) ->
	catch erlang:cancel_timer(SRef),
	ok.
