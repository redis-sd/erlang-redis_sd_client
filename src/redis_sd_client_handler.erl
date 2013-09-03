%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pagodabox.com>
%%% @copyright 2013, Pagoda Box, Inc.
%%% @doc
%%%
%%% @end
%%% Created :  02 Sep 2013 by Andrew Bennett <andrew@pagodabox.com>
%%%-------------------------------------------------------------------
-module(redis_sd_client_handler).

-include("redis_sd_client.hrl").

-type domain()  :: binary().
-type type()    :: binary().
-type service() :: {{binary(), integer()}, [{binary(), binary()}], integer()}.

-callback browse_init(Browse::#browse{})
	-> {ok, State::any()}.
-callback browse_service_add(Domain::domain(), Type::type(), Service::service(), State::any())
	-> {ok, State::any()}.
-callback browse_service_remove(Domain::domain(), Type::type(), Service::service(), State::any())
	-> {ok, State::any()}.
-callback browse_info(Info::any(), State::any())
	-> {ok, State::any()}.
-callback browse_terminate(Reason::any(), State::any())
	-> term().
