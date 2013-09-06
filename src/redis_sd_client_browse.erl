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
handle_cast({service_remove, Record}, Parser=#parser{browse=Browse}) ->
	{ok, Parser2} = browser_service_remove(Record, Parser),
	redis_sd_client_event:service_remove(Record, Browse),
	{noreply, Parser2};
handle_cast(Request, Parser) ->
	error_logger:error_msg(
		"** ~p ~p unhandled request in ~p/~p~n"
		"   Request was: ~p~n",
		[?MODULE, self(), handle_cast, 2, Request]),
	{noreply, Parser}.

%% @private
handle_info('$redis_sd_tick', Parser=#parser{discovered=Discovered, browse=Browse}) ->
	Discovered2 = dict:fold(fun({Domain, Type, Service, Hostname, Instance}, Record=#redis_sd{ttl=TTL}, D) ->
		case TTL - 1 of
			TTL2 when TTL2 =< 0 ->
				ok = gen_server:cast(Browse#browse.name, {service_remove, Record#redis_sd{ttl=TTL2}}),
				D;
			TTL2 ->
				dict:store({Domain, Type, Service, Hostname, Instance}, Record#redis_sd{ttl=TTL2}, D)
		end
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
browser_service_add(_Record, Parser=#parser{browser=undefined}) ->
	{ok, Parser};
browser_service_add(Record, Parser=#parser{browser=Browser, state=State}) ->
	case Browser:browser_service_add(Record, State) of
		{ok, State2} ->
			{ok, Parser#parser{state=State2}}
	end.

%% @private
browser_service_remove(_Record, Parser=#parser{browser=undefined}) ->
	{ok, Parser};
browser_service_remove(Record, Parser=#parser{browser=Browser, state=State}) ->
	case Browser:browser_service_remove(Record, State) of
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
	AnswerTTL = ttl(Answer, TTL),
	R = {iolist_to_binary(AnswerData), iolist_to_binary(AnswerDomain), ResourceTypes},
	S = #redis_sd{ttl=AnswerTTL},
	case handle_answer(R, S) of
		{add, S2=#redis_sd{domain=Domain, type=Type, service=Service, hostname=Hostname, instance=Instance}} ->
			Discovered2 = dict:store({Domain, Type, Service, Hostname, Instance}, S2, Discovered),
			{ok, Parser2} = browser_service_add(S2, Parser#parser{discovered=Discovered2}),
			redis_sd_client_event:service_add(S2, Browse),
			handle_advertisement(Answers, Resources, TTL, Parser2);
		{remove, S2=#redis_sd{domain=Domain, type=Type, service=Service, hostname=Hostname, instance=Instance}} ->
			Discovered2 = dict:erase({Domain, Type, Service, Hostname, Instance}, Discovered),
			{ok, Parser2} = browser_service_remove(S2, Parser#parser{discovered=Discovered2}),
			redis_sd_client_event:service_remove(S2, Browse),
			handle_advertisement(Answers, Resources, TTL, Parser2);
		ignore ->
			handle_advertisement(Answers, Resources, TTL, Parser)
	end.

%% @private
handle_answer({InstKey, TypeKey, Resources}, S=#redis_sd{target=undefined, port=undefined, txtdata=undefined}) ->
	case get_value(txt, Resources) of
		undefined ->
			ignore;
		TXT ->
			case get_value(srv, Resources) of
				undefined ->
					ignore;
				{_Priority, _Weight, Port, TargetString} ->
					Target = iolist_to_binary(TargetString),
					TXTData = parse_txt(TXT, []),
					handle_answer({InstKey, TypeKey}, S#redis_sd{target=Target, port=Port, txtdata=TXTData})
			end
	end;
handle_answer({InstKey, TypeKey}, S=#redis_sd{domain=undefined, type=undefined, service=undefined}) ->
	case parse_service_type_domain(TypeKey) of
		{Service, Type, Domain} ->
			handle_answer({InstKey, TypeKey}, S#redis_sd{domain=Domain, type=Type, service=Service});
		_ ->
			ignore
	end;
handle_answer({InstKey, TypeKey}, S=#redis_sd{ttl=TTL, hostname=undefined, instance=undefined}) ->
	case parse_hostname_instance(InstKey, TypeKey) of
		{Hostname, Instance} ->
			handle_answer(TTL, S#redis_sd{hostname=Hostname, instance=Instance});
		_ ->
			ignore
	end;
handle_answer(TTL, S) when is_integer(TTL) andalso TTL =< 0 ->
	{remove, S};
handle_answer(TTL, S) when is_integer(TTL) ->
	{add, S}.


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
parse_service_type_domain(N) ->
	case redis_sd:nssplit(N) of
		[<< $_, Service/binary >>, << $_, Type/binary >> | DomainLabels] ->
			Domain = redis_sd:nsjoin(DomainLabels),
			{Service, Type, Domain};
		_ ->
			ignore
	end.

%% @private
parse_hostname_instance(InstKey, TypeKey) when is_binary(InstKey) andalso is_binary(TypeKey) ->
	case parse_instance(InstKey, TypeKey) of
		{HostnameInstance, TypeKey} ->
			case parse_hostname(HostnameInstance) of
				{Hostname, Instance} ->
					{Hostname, Instance};
				_ ->
					ignore
			end;
		_ ->
			ignore
	end.

%% @private
parse_instance(InstKey, TypeKey) when is_binary(InstKey) andalso is_binary(TypeKey) ->
	case binary:longest_common_suffix([InstKey, << $., TypeKey/binary >>]) of
		0 ->
			ignore;
		N ->
			{binary:part(InstKey, 0, byte_size(InstKey) - N), TypeKey}
	end.

%% @private
parse_hostname(N) ->
	parse_hostname(redis_sd:nssplit(N), []).

%% @private
parse_hostname([Hostname], Acc) ->
	{Hostname, redis_sd:nsjoin(lists:reverse(Acc))};
parse_hostname([InstLabel | Rest], Acc) ->
	parse_hostname(Rest, [InstLabel | Acc]).

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
