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
-module(redis_sd_client).
-behaviour(gen_server).

-include("redis_sd_client.hrl").

-define(SERVER, ?MODULE).
-define(TAB, ?MODULE).

%% API
-export([manual_start/0, new_browse/1, rm_browse/1, delete_browse/1, list/0, list/1, start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	terminate/2, code_change/3]).

-record(state, {}).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Manually start redis_sd_client and all dependencies.
-spec manual_start() -> ok.
manual_start() ->
	redis_sd:require([
		hierdis,
		redis_sd_client
	]).

new_browse(BrowseConfig) ->
	redis_sd_client_sup:new_browse(BrowseConfig).

%% @doc Gracefully terminate the named browse.
rm_browse(BrowseName) ->
	redis_sd_client_sup:rm_browse(BrowseName).

%% @doc Forcefully terminate the named browse.
delete_browse(BrowseName) ->
	redis_sd_client_sup:delete_browse(BrowseName).

list() ->
	ets:match_object(?TAB, '$0').

list(BrowseName) ->
	ets:match_object(?TAB, {{{browse, BrowseName}, '_'}, '_', '_', '_', '_'}).

%% @private
start_link() ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
	ok = redis_sd_client_event:add_handler(redis_sd_event_handler, self()),
	State = #state{},
	{ok, State}.

%% @private
handle_call(_Request, _From, State) ->
	{reply, ignore, State}.

%% @private
handle_cast(_Request, State) ->
	{noreply, State}.

%% @private
handle_info({'$redis_sd', {browse, terminate, normal, #browse{name=Name}}}, State) ->
	ok = redis_sd_client:delete_browse(Name),
	ets:match_delete(?TAB, {{{browse, Name}, '_'}, '_', '_', '_', '_'}),
	{noreply, State};
handle_info({'$redis_sd', {service, add, #redis_sd{ttl=TTL, domain=D, type=T, service=S, hostname=H, instance=I, target=Target, port=Port, txtdata=TXTData}, #browse{name=N}}}, State) ->
	Key = {{browse, N}, {D, T, S, H, I}},
	Object = {Key, TTL, Target, Port, TXTData},
	ets:insert(?TAB, Object),
	{noreply, State};
handle_info({'$redis_sd', {service, remove, #redis_sd{domain=D, type=T, service=S, hostname=H, instance=I}, #browse{name=N}}}, State) ->
	Key = {{browse, N}, {D, T, S, H, I}},
	ets:delete(?TAB, Key),
	{noreply, State};
handle_info({'$redis_sd', _Event}, State) ->
	{noreply, State};
handle_info(Info, State) ->
	error_logger:error_msg(
		"** ~p ~p unhandled info in ~p/~p~n"
		"   Info was: ~p~n",
		[?MODULE, self(), handle_info, 2, Info]),
	{noreply, State}.

%% @private
terminate(_Reason, _State) ->
	ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
