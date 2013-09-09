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
-module(redis_sd_browser).

-include("redis_sd_client.hrl").

-callback browser_init(Browse::#browse{}, Options::any())
	-> {ok, State::any()}.
-callback browser_service_add(Record::redis_sd:obj(), State::any())
	-> {ok, State::any()}.
-callback browser_service_remove(Record::redis_sd:obj(), State::any())
	-> {ok, State::any()}.
-callback browser_call(Request::any(), From::{pid(), reference()}, State::any())
	-> {noreply, State::any()}
	| {reply, Reply::any(), State::any()}.
-callback browser_info(Info::any(), State::any())
	-> {ok, State::any()}.
-callback browser_terminate(Reason::any(), State::any())
	-> term().

%% API
-export([call/2, call/3, reply/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

call(Name, Request) ->
	case catch gen:call(Name, '$redis_sd_browser_call', Request) of
		{ok, Reply} ->
			Reply;
		{'EXIT', Reason} ->
			erlang:exit({Reason, {?MODULE, call, [Name, Request]}})
	end.

call(Name, Request, Timeout) ->
	case catch gen:call(Name, '$redis_sd_browser_call', Request, Timeout) of
		{ok, Reply} ->
			Reply;
		{'EXIT', Reason} ->
			erlang:exit({Reason, {?MODULE, call, [Name, Request, Timeout]}})
	end.

reply({To, Tag}, Reply) ->
	catch To ! {Tag, Reply}.
