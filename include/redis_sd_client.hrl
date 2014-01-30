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

-ifndef(REDIS_SD_CLIENT_HRL).

-include_lib("redis_sd_spec/include/redis_sd.hrl").

-type redis_sd_browse_pattern() :: iodata() | [iodata() | '*' | '?' | atom()].

-record(redis_sd_browse_v1, {
	enabled = true :: boolean(),

	ref      = undefined :: undefined | integer(),
	domain   = undefined :: undefined | redis_sd_browse_pattern(),
	type     = undefined :: undefined | redis_sd_browse_pattern(),
	service  = undefined :: undefined | redis_sd_browse_pattern(),
	instance = undefined :: undefined | redis_sd_browse_pattern(),
	greedy   = true      :: boolean(),

	%% Redis Options
	redis_opts = {tcp, ["127.0.0.1", 6379]} :: {tcp | unix, [string() | integer() | timeout()]},
	redis_auth = undefined                  :: undefined | iodata(),
	redis_ns   = ""                         :: iodata(),
	redis_cli  = undefined                  :: undefined | port(),
	redis_sub  = undefined                  :: undefined | port(),

	%% Redis Commands
	cmd_auth         = "AUTH"         :: iodata(),
	cmd_keys         = "KEYS"         :: iodata(),
	cmd_mget         = "MGET"         :: iodata(),
	cmd_psubscribe   = "PSUBSCRIBE"   :: iodata(),
	cmd_punsubscribe = "PUNSUBSCRIBE" :: iodata(),
	cmd_ttl          = "TTL"          :: iodata(),

	%% Reconnect Options
	min_wait = 1         :: integer(), % seconds
	max_wait = 120       :: integer(), % seconds
	backoff  = undefined :: undefined | backoff:backoff(),
	bref     = undefined :: undefined | reference(),
	sync     = undefined :: undefined | {pid(), reference()},
	table    = undefined :: undefined | ets:tid(),

	%% Pattern Cache
	channels = undefined :: undefined | [binary()]
}).

-type redis_sd_browse() :: #redis_sd_browse_v1{}.

-define(REDIS_SD_BROWSE, #redis_sd_browse_v1).

-define(REDIS_SD_CLIENT_HRL, 1).

-endif.
