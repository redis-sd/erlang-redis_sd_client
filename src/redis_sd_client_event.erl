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
-module(redis_sd_client_event).

-include("redis_sd_client.hrl").

-define(MANAGER, redis_sd_client_manager).

%% API
-export([manager/0, add_handler/2]).
-export([browse_init/1, browse_enable/1, browse_disable/1,
	browse_connect/1, browse_subscribe/2, browse_terminate/2]).
-export([record_add/3, record_expire/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

manager() ->
	?MANAGER.

add_handler(Handler, Pid) ->
	gen_event:add_handler(manager(), Handler, Pid).

browse_init(Browse=?REDIS_SD_BROWSE{}) ->
	notify({browse, init, Browse}).

browse_enable(Browse=?REDIS_SD_BROWSE{}) ->
	notify({browse, enable, Browse}).

browse_disable(Browse=?REDIS_SD_BROWSE{}) ->
	notify({browse, disable, Browse}).

browse_connect(Browse=?REDIS_SD_BROWSE{}) ->
	notify({browse, connect, Browse}).

browse_subscribe(Channel, Browse=?REDIS_SD_BROWSE{}) ->
	notify({browse, subscribe, Channel, Browse}).

browse_terminate(Reason, Browse=?REDIS_SD_BROWSE{}) ->
	notify({browse, terminate, Reason, Browse}).

record_add(Key, Record, Browse=?REDIS_SD_BROWSE{}) ->
	notify({record, add, Key, Record, Browse}).

record_expire(Key, Record, Browse=?REDIS_SD_BROWSE{}) ->
	notify({record, expire, Key, Record, Browse}).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
notify(Message) ->
	gen_event:notify(manager(), Message).
