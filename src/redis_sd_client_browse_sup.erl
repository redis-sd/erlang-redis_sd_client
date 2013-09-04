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
-module(redis_sd_client_browse_sup).
-behaviour(supervisor).

-include("redis_sd_client.hrl").

%% API
-export([start_link/1, start_sync/1, browse_sup_name/1, browse_reader_name/1, graceful_shutdown/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Browse=#browse{}) ->
	SupName = browse_sup_name(Browse),
	supervisor:start_link({local, SupName}, ?MODULE, Browse).

start_sync(Browse=#browse{}) ->
	SupName = browse_sup_name(Browse),
	SyncSpec = sync_spec(Browse),
	supervisor:start_child(SupName, SyncSpec).

browse_sup_name(#browse{name=Name}) ->
	list_to_atom("redis_sd_client_" ++ atom_to_list(Name) ++ "_browse_sup").

browse_reader_name(#browse{name=Name}) ->
	list_to_atom("redis_sd_client_" ++ atom_to_list(Name) ++ "_browse_reader").

%% @doc Gracefully shutdown the named browse.
graceful_shutdown(Name) ->
	case catch redis_sd_client_browse:graceful_shutdown(Name) of
		ok ->
			ok;
		_ ->
			forced_shutdown
	end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(Browse=#browse{}) ->
	ReaderName = browse_reader_name(Browse),
	Browse2 = Browse#browse{reader=ReaderName},
	BrowseSpec = {redis_sd_client_browse,
		{redis_sd_client_browse, start_link, [Browse2]},
		transient, 2000, worker, [redis_sd_client_browse]},
	ReaderSpec = {ReaderName,
		{redis_sd_client_browse_reader, start_link, [Browse2]},
		transient, 2000, worker, [redis_sd_client_browse_reader]},
	%% five restarts in 60 seconds, then shutdown
	Restart = {one_for_all, 5, 60},
	{ok, {Restart, [BrowseSpec, ReaderSpec]}}.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
sync_spec(Browse=#browse{}) ->
	{redis_sd_client_browse_sync,
		{redis_sd_client_browse_sync, start_link, [Browse]},
		temporary, brutal_kill, worker, [redis_sd_client_browse_sync]}.
