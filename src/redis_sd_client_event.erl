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
-module(redis_sd_client_event).

-include("redis_sd_client.hrl").

%% API
-export([manager/0, add_handler/2]).
-export([browse_init/1, browse_connect/1, browse_subscribe/2, browse_terminate/2]).
-export([service_add/4, service_remove/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

manager() ->
	redis_sd_client_manager.

add_handler(Handler, Pid) ->
	gen_event:add_handler(manager(), Handler, Pid).

browse_init(Browse=#browse{}) ->
	notify({browse, init, Browse}).

browse_connect(Browse=#browse{}) ->
	notify({browse, connect, Browse}).

browse_subscribe(Channel, Browse=#browse{}) ->
	notify({browse, subscribe, Channel, Browse}).

browse_terminate(Reason, Browse=#browse{}) ->
	notify({browse, terminate, Reason, Browse}).

service_add(Domain, Type, Service, Browse=#browse{}) ->
	notify({service, add, Domain, Type, Service, Browse}).

service_remove(Domain, Type, Service, Browse=#browse{}) ->
	notify({service, remove, Domain, Type, Service, Browse}).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
notify(Message) ->
	gen_event:notify(manager(), Message).
