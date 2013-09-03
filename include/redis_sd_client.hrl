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

-type browse_pattern() :: string() | [string() | '*' | '?' | atom()].

-record(browse, {
	name     = undefined :: undefined | atom(),
	hostname = undefined :: undefined | browse_pattern(),
	service  = undefined :: undefined | browse_pattern(),
	type     = undefined :: undefined | browse_pattern(),
	domain   = undefined :: undefined | browse_pattern(),
	greedy   = true      :: boolean(),
	handler  = undefined :: undefined | module(),

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
	aref     = undefined :: undefined | reference(),
	sref     = undefined :: undefined | reference(),
	reader   = undefined :: undefined | atom(),

	%% Pattern Cache
	h_glob  = undefined :: undefined | binary(),
	s_glob  = undefined :: undefined | binary(),
	t_glob  = undefined :: undefined | binary(),
	d_glob  = undefined :: undefined | binary(),
	g_glob  = undefined :: undefined | binary(),
	glob    = undefined :: undefined | binary(),
	channel = undefined :: undefined | binary()
}).
