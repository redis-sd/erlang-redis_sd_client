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
-module(redis_sd_client_sup).
-behaviour(supervisor).

-include("redis_sd_client.hrl").

%% API
-export([start_link/0, new_browse/1, rm_browse/1, delete_browse/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Create a new browse from proplist browse config `BrowseConfig'. The
%% public API for this functionality is {@link redis_sd_client:new_browse/1}.
new_browse(BrowseConfig) when is_list(BrowseConfig) ->
	new_browse(redis_sd_client_config:list_to_browse(BrowseConfig));
new_browse(Browse=?REDIS_SD_BROWSE{}) ->
	Spec = browse_sup_spec(Browse),
	supervisor:start_child(?MODULE, Spec).

%% @doc Gracefully shutdown the named browse.
rm_browse(Name) ->
	case redis_sd_client_browse_sup:graceful_shutdown(Name) of
		ok ->
			ok;
		forced_shutdown ->
			delete_browse(Name)
	end.

%% @doc Forcefully shutdown the named browse.
delete_browse(Name) ->
	SupName = browse_sup_name(Name),
	case supervisor:terminate_child(?MODULE, SupName) of
		{error, not_found} ->
			ok;
		ok ->
			supervisor:delete_child(?MODULE, SupName);
		Error ->
			Error
	end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
	redis_sd_client = ets:new(redis_sd_client, [ordered_set, public, named_table]),
	ManagerSpec = {redis_sd_client_event:manager(),
		{gen_event, start_link, [{local, redis_sd_client_event:manager()}]},
		permanent, 5000, worker, [gen_event]},
	ClientSpec = {redis_sd_client,
		{redis_sd_client, start_link, []},
		permanent, 5000, worker, [redis_sd_client]},

	%% a list of browse configs
	Config = case application:get_env(redis_sd_client, browses) of
		{ok, C} ->
			C;
		undefined ->
			[]
	end,
	Browses = [redis_sd_client_config:list_to_browse(L) || L <- Config],
	BrowseSupSpecs = [browse_sup_spec(Browse) || Browse <- Browses],

	%% five restarts in 60 seconds, then shutdown
	Restart = {rest_for_one, 5, 60},
	{ok, {Restart, [ManagerSpec, ClientSpec | BrowseSupSpecs]}}.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
browse_sup_spec(Browse=?REDIS_SD_BROWSE{ref=Ref}) ->
	SupName = browse_sup_name(Ref),
	{SupName,
		{redis_sd_client_browse_sup, start_link, [Browse]},
		transient, 5000, supervisor, [redis_sd_client_browse_sup]}.

%% @private
browse_sup_name(Ref) ->
	list_to_atom("redis_sd_client_" ++ integer_to_list(Ref) ++ "_browse_sup").
