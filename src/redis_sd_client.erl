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

%% API
-export([manual_start/0, new_browse/1, rm_browse/1, delete_browse/1, list/0, start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	terminate/2, code_change/3]).

-record(state, {
	discovered = dict:new() :: dict()
}).

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
	gen_server:call(?SERVER, list).

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
handle_call(list, _From, State=#state{discovered=Discovered}) ->
	{reply, dict:to_list(Discovered), State};
handle_call(_Request, _From, State) ->
	Reply = {ok, _Request},
	{reply, Reply, State}.

%% @private
handle_cast(_Request, State) ->
	{noreply, State}.

%% @private
handle_info({'$redis_sd', {browse, terminate, normal, #browse{name=Name}}}, State=#state{discovered=Discovered}) ->
	ok = redis_sd_client:delete_browse(Name),
	Discovered2 = dict:fold(fun
		({N, _Domain, _Type}, _V, D) when N =:= Name ->
			D;
		(K, V, D) ->
			dict:store(K, V, D)
	end, dict:new(), Discovered),
	{noreply, State#state{discovered=Discovered2}};
handle_info({'$redis_sd', {service, add, Domain, Type, {{Target, Port}, Options, _TTL}, #browse{name=Name}}}, State=#state{discovered=Discovered}) ->
	Service = {{Target, Port}, Options},
	Discovered2 = dict:update({Name, Domain, Type}, fun(Services) ->
		lists:keystore({Target, Port}, 1, Services, Service)
	end, [Service], Discovered),
	{noreply, State#state{discovered=Discovered2}};
handle_info({'$redis_sd', {service, remove, Domain, Type, {{Target, Port}, _Options, _TTL}, #browse{name=Name}}}, State=#state{discovered=Discovered}) ->
	Discovered2 = dict:update({Name, Domain, Type}, fun(Services) ->
		lists:keydelete({Target, Port}, 1, Services)
	end, [], Discovered),
	{noreply, State#state{discovered=Discovered2}};
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
