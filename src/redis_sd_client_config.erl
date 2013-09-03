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

-spec list_to_browse([{atom(), term()}]) -> #browse{}.
list_to_browse(B) ->
	Default = #browse{},
	#browse{
		name     = req(name, B),
		hostname = opt(hostname, B, Default#browse.hostname),
		service  = opt(service, B, Default#browse.service),
		type     = opt(type, B, Default#browse.type),
		domain   = opt(domain, B, Default#browse.domain),
		greedy   = opt(greedy, B, Default#browse.greedy),
		handler  = opt(handler, B, Default#browse.handler),

		%% Redis Options
		redis_opts = opt(redis_opts, B, Default#browse.redis_opts),
		redis_auth = opt(redis_auth, B, Default#browse.redis_auth),
		redis_ns   = opt(redis_ns, B, Default#browse.redis_ns),

		%% Redis Commands
		cmd_auth         = opt(cmd_auth, B, Default#browse.cmd_auth),
		cmd_keys         = opt(cmd_keys, B, Default#browse.cmd_keys),
		cmd_mget         = opt(cmd_mget, B, Default#browse.cmd_mget),
		cmd_psubscribe   = opt(cmd_psubscribe, B, Default#browse.cmd_psubscribe),
		cmd_punsubscribe = opt(cmd_punsubscribe, B, Default#browse.cmd_punsubscribe),
		cmd_ttl          = opt(cmd_ttl, B, Default#browse.cmd_ttl),

		%% Reconnect Options
		min_wait = opt(min_wait, B, Default#browse.min_wait),
		max_wait = opt(max_wait, B, Default#browse.max_wait)
	}.

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
