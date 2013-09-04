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
-module(redis_sd_client_browse).
-behaviour(gen_server).

-include("redis_sd_client.hrl").

%% API
-export([start_link/1, parse/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	terminate/2, code_change/3]).

-record(parser, {
	browse     = undefined  :: undefined | #browse{},
	browser    = undefined  :: undefined | module(),
	state      = undefined  :: undefined | any(),
	discovered = dict:new() :: dict()
}).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @private
start_link(Browse=#browse{name=Name}) ->
	gen_server:start_link({local, Name}, ?MODULE, Browse, []).

parse(#browse{name=Name}, KeyVals) ->
	gen_server:cast(Name, {parse, KeyVals}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init(Browse=#browse{browser=Browser, browser_opts=BrowserOpts}) ->
	Parser = #parser{browse=Browse, browser=Browser},
	{ok, Parser2} = browser_init(BrowserOpts, Parser),
	timer:send_interval(1000, '$redis_sd_tick'),
	{ok, Parser2}.

%% @private
handle_call(_Request, _From, Parser) ->
	Reply = ok,
	{reply, Reply, Parser}.

%% @private
handle_cast({parse, KeyVals}, Parser) ->
	handle_parse(KeyVals, Parser);
handle_cast({service_remove, Domain, Type, Service}, Parser=#parser{browse=Browse}) ->
	{ok, Parser2} = browser_service_remove(Domain, Type, Service, Parser),
	redis_sd_client_event:service_remove(Domain, Type, Service, Browse),
	{noreply, Parser2};
handle_cast(Request, Parser) ->
	error_logger:error_msg(
		"** ~p ~p unhandled request in ~p/~p~n"
		"   Request was: ~p~n",
		[?MODULE, self(), handle_cast, 2, Request]),
	{noreply, Parser}.

%% @private
handle_info('$redis_sd_tick', Parser=#parser{discovered=Discovered, browse=Browse}) ->
	Discovered2 = dict:fold(fun({Domain, Type}, V, D) ->
		dict:store({Domain, Type}, lists:foldl(fun({Instance, Target, Port, Options, TTL}, L) ->
			case TTL - 1 of
				TTL2 when TTL2 =< 0 ->
					ok = gen_server:cast(Browse#browse.name, {service_remove, Domain, Type, {Instance, Target, Port, Options, TTL2}}),
					L;
				TTL2 ->
					[{Instance, Target, Port, Options, TTL2} | L]
			end
		end, [], V), D)
	end, dict:new(), Discovered),
	{noreply, Parser#parser{discovered=Discovered2}};
handle_info({'$redis_sd_browser_call', From, Request}, Parser) ->
	{ok, Parser2} = browser_call(Request, From, Parser),
	{noreply, Parser2};
handle_info(Info, Parser) ->
	{ok, Parser2} = browser_info(Info, Parser),
	{noreply, Parser2}.

%% @private
terminate(Reason, Parser) ->
	browser_terminate(Reason, Parser).

%% @private
code_change(_OldVsn, Parser, _Extra) ->
	{ok, Parser}.

%%%-------------------------------------------------------------------
%%% Browser functions
%%%-------------------------------------------------------------------

%% @private
browser_init(_BrowserOpts, Parser=#parser{browser=undefined}) ->
	{ok, Parser};
browser_init(BrowserOpts, Parser=#parser{browser=Browser, browse=Browse}) ->
	case Browser:browser_init(Browse, BrowserOpts) of
		{ok, State} ->
			{ok, Parser#parser{state=State}}
	end.

%% @private
browser_service_add(_Domain, _Type, _Service, Parser=#parser{browser=undefined}) ->
	{ok, Parser};
browser_service_add(Domain, Type, Service, Parser=#parser{browser=Browser, state=State}) ->
	case Browser:browser_service_add(Domain, Type, Service, State) of
		{ok, State2} ->
			{ok, Parser#parser{state=State2}}
	end.

%% @private
browser_service_remove(_Domain, _Type, _Service, Parser=#parser{browser=undefined}) ->
	{ok, Parser};
browser_service_remove(Domain, Type, Service, Parser=#parser{browser=Browser, state=State}) ->
	case Browser:browser_service_remove(Domain, Type, Service, State) of
		{ok, State2} ->
			{ok, Parser#parser{state=State2}}
	end.

%% @private
browser_call(_Request, From, Parser=#parser{browser=undefined}) ->
	_ = redis_sd_browser:reply(From, {error, no_browser_defined}),
	{ok, Parser};
browser_call(Request, From, Parser=#parser{browser=Browser, state=State}) ->
	case Browser:browser_call(Request, From, State) of
		{noreply, State2} ->
			{ok, Parser#parser{state=State2}};
		{reply, Reply, State2} ->
			_ = redis_sd_browser:reply(From, Reply),
			{ok, Parser#parser{state=State2}}
	end.

%% @private
browser_info(_Info, Parser=#parser{browser=undefined}) ->
	{ok, Parser};
browser_info(Info, Parser=#parser{browser=Browser, state=State}) ->
	case Browser:browser_info(Info, State) of
		{ok, State2} ->
			{ok, Parser#parser{state=State2}}
	end.

%% @private
browser_terminate(_Reason, #parser{browser=undefined}) ->
	ok;
browser_terminate(Reason, #parser{browser=Browser, state=State}) ->
	_ = Browser:browser_terminate(Reason, State),
	ok.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
handle_parse([], Parser) ->
	{noreply, Parser};
handle_parse([{Key, Val, TTL} | KeyVals], Parser) ->
	{Header, Type, Questions, Answers, Authorities, Resources} = parse_record(Val),
	QR = proplists:get_value(qr, Header),
	Opcode = proplists:get_value(opcode, Header),
	Data = {Questions, Answers, Authorities, Resources},
	case handle_record(Key, Header, Type, QR, Opcode, Data, TTL, Parser) of
		{ok, Parser2} ->
			handle_parse(KeyVals, Parser2);
		{stop, Reason, Parser2} ->
			{stop, Reason, Parser2}
	end.

%% @private
parse_record(Record) ->
	Header = inet_dns:header(inet_dns:msg(Record, header)),
	Type = inet_dns:record_type(Record),
	Questions = [inet_dns:dns_query(Query) || Query <- inet_dns:msg(Record, qdlist)],
	Answers = [inet_dns:rr(RR) || RR <- inet_dns:msg(Record, anlist)],
	Authorities = [inet_dns:rr(RR) || RR <- inet_dns:msg(Record, nslist)],
	Resources = [inet_dns:rr(RR) || RR <- inet_dns:msg(Record, arlist)],
	{Header, Type, Questions, Answers, Authorities, Resources}.

%% @private
handle_record(_Key, _Header, msg, true, 'query', {[], Answers, [], Resources}, TTL, Parser) ->
	handle_advertisement(Answers, Resources, TTL, Parser);
handle_record(_Key, _Header, _Type, _QR, _Opcode, _Data, _TTL, Parser) ->
	{ok, Parser}.

%% @private
handle_advertisement([], _Resources, _TTL, Parser) ->
	{ok, Parser};
handle_advertisement([Answer | Answers], Resources, TTL, Parser=#parser{discovered=Discovered, browse=Browse}) ->
	AnswerData = data(Answer),
	AnswerDomain = domain(Answer),
	ResourceTypes = [{type(Resource), data(Resource)} || Resource <- Resources, domain(Resource) =:= AnswerData],
	case get_value(txt, ResourceTypes) of
		undefined ->
			handle_advertisement(Answers, Resources, TTL, Parser);
		TXT ->
			case get_value(srv, ResourceTypes) of
				undefined ->
					handle_advertisement(Answers, Resources, TTL, Parser);
				{_Priority, _Weight, Port, TargetString} ->
					Target = iolist_to_binary(TargetString),
					{Type, Domain} = parse_type_domain(AnswerDomain),
					{Instance, _TypeKey} = parse_instance(AnswerData, AnswerDomain),
					Options = parse_txt(TXT, []),
					case ttl(Answer, TTL) of
						AnswerTTL when is_integer(AnswerTTL) andalso AnswerTTL =< 0 -> % Remove request
							Service = {Instance, Target, Port, Options, AnswerTTL},
							Discovered2 = dict:update({Domain, Type}, fun(Services) ->
								lists:keydelete(Instance, 1, Services)
							end, [], Discovered),
							{ok, Parser2} = browser_service_remove(Domain, Type, Service, Parser#parser{discovered=Discovered2}),
							redis_sd_client_event:service_remove(Domain, Type, Service, Browse),
							handle_advertisement(Answers, Resources, TTL, Parser2);
						AnswerTTL when is_integer(AnswerTTL) -> % Add request
							Service = {Instance, Target, Port, Options, AnswerTTL},
							Discovered2 = dict:update({Domain, Type}, fun(Services) ->
								lists:keystore(Instance, 1, Services, Service)
							end, [Service], Discovered),
							{ok, Parser2} = browser_service_add(Domain, Type, Service, Parser#parser{discovered=Discovered2}),
							redis_sd_client_event:service_add(Domain, Type, Service, Browse),
							handle_advertisement(Answers, Resources, TTL, Parser2)
					end
			end
	end.

%% @private
get_value(Key, Opts) ->
	get_value(Key, Opts, undefined).

%% @doc Faster alternative to proplists:get_value/3.
%% @private
get_value(Key, Opts, Default) ->
	case lists:keyfind(Key, 1, Opts) of
		{_, Value} -> Value;
		_ -> Default
	end.

%% @private
domain(Resource) ->
	get_value(domain, Resource).

%% @private
type(Resource) ->
	get_value(type, Resource).

%% @private
data(Resource) ->
	get_value(data, Resource).

%% @private
ttl(_Resource, TTL) when is_integer(TTL) ->
	TTL;
ttl(Resource, undefined) ->
	get_value(ttl, Resource).

%% @private
parse_type_domain(Name) ->
	case redis_sd:nssplit(Name) of
		[Service, Type | DomainParts] ->
			ServiceType = redis_sd:nsjoin([Service, Type]),
			Domain = redis_sd:nsjoin(DomainParts),
			{ServiceType, Domain};
		_ ->
			{<<>>, <<>>}
	end.

%% @private
parse_instance(InstKey, TypeKey) when is_list(InstKey) ->
	parse_instance(iolist_to_binary(InstKey), TypeKey);
parse_instance(InstKey, TypeKey) when is_list(TypeKey) ->
	parse_instance(InstKey, iolist_to_binary(TypeKey));
parse_instance(InstKey, TypeKey) ->
	{binary:part(InstKey, 0, byte_size(InstKey) - binary:longest_common_suffix([InstKey, << $., TypeKey/binary >>])), TypeKey}.

%% @private
parse_txt([], Acc) ->
	lists:reverse(Acc);
parse_txt([TXT | TXTs], Acc) ->
	case binary:split(iolist_to_binary(TXT), <<"=">>) of
		[Key, Val] ->
			parse_txt(TXTs, [{redis_sd:urldecode(Key), redis_sd:urldecode(Val)} | Acc]);
		_ ->
			parse_txt(TXTs, Acc)
	end.
