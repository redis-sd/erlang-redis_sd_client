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
-module(redis_sd_client_config).

-include("redis_sd_client.hrl").

%% API
-export([list_to_browse/1, list_to_browse/2]).

%% Internal
-export([opt/2, opt/3, req/2]).
-ignore_xref([opt/2, opt/3, req/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec list_to_browse([{atom(), term()}]) -> redis_sd_browse().
list_to_browse(BrowseConfig) ->
	list_to_browse([], BrowseConfig).

-spec list_to_browse([module()], [{atom(), term()}]) -> redis_sd_browse().
list_to_browse(Apps, BrowseConfig) when is_list(Apps) ->
	Default = ?REDIS_SD_BROWSE{},
	Defaults = [
		{enabled, Default?REDIS_SD_BROWSE.enabled, app},
		{greedy, Default?REDIS_SD_BROWSE.greedy, app},
		{redis_opts, Default?REDIS_SD_BROWSE.redis_opts, app},
		{redis_auth, Default?REDIS_SD_BROWSE.redis_auth, app},
		{redis_ns, Default?REDIS_SD_BROWSE.redis_ns, app},
		{cmd_auth, Default?REDIS_SD_BROWSE.cmd_auth, app},
		{cmd_keys, Default?REDIS_SD_BROWSE.cmd_keys, app},
		{cmd_mget, Default?REDIS_SD_BROWSE.cmd_mget, app},
		{cmd_psubscribe, Default?REDIS_SD_BROWSE.cmd_psubscribe, app},
		{cmd_punsubscribe, Default?REDIS_SD_BROWSE.cmd_punsubscribe, app},
		{cmd_ttl, Default?REDIS_SD_BROWSE.cmd_ttl, app},
		{min_wait, Default?REDIS_SD_BROWSE.min_wait, app},
		{max_wait, Default?REDIS_SD_BROWSE.max_wait, app}
	],
	B = redis_sd_config:merge(Apps ++ [redis_sd_client], Defaults, BrowseConfig),
	Enabled = req(enabled, B),
	case Enabled of
		false ->
			ok;
		true ->
			ok;
		BadBoolean ->
			erlang:error({invalid_enabled_boolean, {enabled, BadBoolean}, B})
	end,
	B1 = ?REDIS_SD_BROWSE{
		enabled = Enabled,

		domain   = opt(domain, B, Default?REDIS_SD_BROWSE.domain),
		type     = opt(type, B, Default?REDIS_SD_BROWSE.type),
		service  = opt(service, B, Default?REDIS_SD_BROWSE.service),
		instance = opt(instance, B, Default?REDIS_SD_BROWSE.instance),
		greedy   = req(greedy, B),

		%% Redis Options
		redis_opts = req(redis_opts, B),
		redis_auth = req(redis_auth, B),
		redis_ns   = req(redis_ns, B),

		%% Redis Commands
		cmd_auth         = req(cmd_auth, B),
		cmd_keys         = req(cmd_keys, B),
		cmd_mget         = req(cmd_mget, B),
		cmd_psubscribe   = req(cmd_psubscribe, B),
		cmd_punsubscribe = req(cmd_punsubscribe, B),
		cmd_ttl          = req(cmd_ttl, B),

		%% Reconnect Options
		min_wait = req(min_wait, B),
		max_wait = req(max_wait, B)
	},
	B1?REDIS_SD_BROWSE{ref=erlang:phash2(B1)}.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
opt(Key, P) ->
	opt(Key, P, undefined).

%% @private
opt(Key, P, Default) ->
	case lists:keyfind(Key, 1, P) of
		false ->
			Default;
		{Key, Value} ->
			Value
	end.

%% @doc Return `Value' for `Key' in proplist `P' or crashes with an
%% informative message if no value is found.
%% @private
req(Key, P) ->
	case lists:keyfind(Key, 1, P) of
		false ->
			erlang:error({missing_required_config, Key, P});
		{Key, Value} ->
			Value
	end.
