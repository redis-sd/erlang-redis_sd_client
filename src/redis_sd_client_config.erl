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
-export([list_to_browse/1]).

%% Internal
-export([opt/2, opt/3, req/2]).
-ignore_xref([opt/2, opt/3, req/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec list_to_browse([{atom(), term()}]) -> ?REDIS_SD_BROWSE{}.
list_to_browse(B) ->
	Default = ?REDIS_SD_BROWSE{},
	Enabled = opt(enabled, B, Default?REDIS_SD_BROWSE.enabled),
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
		greedy   = opt(greedy, B, Default?REDIS_SD_BROWSE.greedy),

		%% Redis Options
		redis_opts = opt(redis_opts, B, Default?REDIS_SD_BROWSE.redis_opts),
		redis_auth = opt(redis_auth, B, Default?REDIS_SD_BROWSE.redis_auth),
		redis_ns   = opt(redis_ns, B, Default?REDIS_SD_BROWSE.redis_ns),

		%% Redis Commands
		cmd_auth         = opt(cmd_auth, B, Default?REDIS_SD_BROWSE.cmd_auth),
		cmd_keys         = opt(cmd_keys, B, Default?REDIS_SD_BROWSE.cmd_keys),
		cmd_mget         = opt(cmd_mget, B, Default?REDIS_SD_BROWSE.cmd_mget),
		cmd_psubscribe   = opt(cmd_psubscribe, B, Default?REDIS_SD_BROWSE.cmd_psubscribe),
		cmd_punsubscribe = opt(cmd_punsubscribe, B, Default?REDIS_SD_BROWSE.cmd_punsubscribe),
		cmd_ttl          = opt(cmd_ttl, B, Default?REDIS_SD_BROWSE.cmd_ttl),

		%% Reconnect Options
		min_wait = opt(min_wait, B, Default?REDIS_SD_BROWSE.min_wait),
		max_wait = opt(max_wait, B, Default?REDIS_SD_BROWSE.max_wait)
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
