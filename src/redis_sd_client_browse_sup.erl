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
-module(redis_sd_client_browse_sup).
-behaviour(supervisor).

-include("redis_sd_client.hrl").

%% API
-export([start_link/1, start_sync/1, browse_sup_name/1, graceful_shutdown/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Browse=?REDIS_SD_BROWSE{}) ->
	SupName = browse_sup_name(Browse),
	supervisor:start_link({local, SupName}, ?MODULE, Browse).

start_sync(Browse=?REDIS_SD_BROWSE{}) ->
	SupName = browse_sup_name(Browse),
	SyncSpec = sync_spec(Browse),
	supervisor:start_child(SupName, SyncSpec).

browse_sup_name(?REDIS_SD_BROWSE{ref=Ref}) ->
	browse_sup_name(Ref);
browse_sup_name(Ref) when is_integer(Ref) ->
	list_to_atom("redis_sd_client_" ++ integer_to_list(Ref) ++ "_browse_sup").

%% @doc Gracefully shutdown the named browse.
graceful_shutdown(BrowseRef) ->
	case catch redis_sd_client_browse:graceful_shutdown(BrowseRef) of
		ok ->
			ok;
		_ ->
			forced_shutdown
	end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(Browse=?REDIS_SD_BROWSE{}) ->
	StateSpec = {redis_sd_client_browse_state,
		{redis_sd_client_browse_state, start_link, [Browse]},
		transient, 2000, worker, [redis_sd_client_browse_state]},
	BrowseSpec = {redis_sd_client_browse,
		{redis_sd_client_browse, start_link, [Browse]},
		transient, 2000, worker, [redis_sd_client_browse]},
	%% five restarts in 60 seconds, then shutdown
	Restart = {one_for_all, 5, 60},
	{ok, {Restart, [StateSpec, BrowseSpec]}}.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
sync_spec(Browse=?REDIS_SD_BROWSE{}) ->
	{redis_sd_client_browse_sync,
		{redis_sd_client_browse_sync, start_link, [Browse]},
		temporary, brutal_kill, worker, [redis_sd_client_browse_sync]}.
