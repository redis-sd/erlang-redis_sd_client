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
-module(redis_sd_client_reader).
-behaviour(gen_server).

-include("redis_sd_client.hrl").

%% API
-export([start_link/1, read/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	terminate/2, code_change/3]).

-record(reader, {
	browse     = undefined  :: undefined | #browse{},
	handler    = undefined  :: undefined | module(),
	state      = undefined  :: undefined | any(),
	discovered = dict:new() :: dict()
}).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @private
start_link(Browse=#browse{reader=ReaderName}) ->
	gen_server:start_link({local, ReaderName}, ?MODULE, Browse, []).

read(#browse{reader=ReaderName}, KeyVals) ->
	gen_server:cast(ReaderName, {read, KeyVals}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init(Browse=#browse{handler=Handler}) ->
	Reader = #reader{browse=Browse, handler=Handler},
	case handler_init(Reader) of
		{ok, Reader2} ->
			timer:send_interval(1000, '$redis_sd_tick'),
			{ok, Reader2};
		{stop, Reason} ->
			{stop, Reason}
	end.

%% @private
handle_call(_Request, _From, Reader) ->
	Reply = ok,
	{reply, Reply, Reader}.

%% @private
handle_cast({read, KeyVals}, Reader) ->
	handle_read(KeyVals, Reader);
handle_cast({service_remove, Domain, Type, Service}, Reader=#reader{browse=Browse}) ->
	{ok, Reader2} = handler_remove(Domain, Type, Service, Reader),
	redis_sd_client_event:service_remove(Domain, Type, Service, Browse),
	{noreply, Reader2};
handle_cast(Request, Reader) ->
	error_logger:error_msg(
		"** ~p ~p unhandled request in ~p/~p~n"
		"   Request was: ~p~n",
		[?MODULE, self(), handle_cast, 2, Request]),
	{noreply, Reader}.

%% @private
handle_info('$redis_sd_tick', Reader=#reader{discovered=Discovered, browse=Browse}) ->
	Discovered2 = dict:fold(fun({Domain, Type}, V, D) ->
		dict:store({Domain, Type}, lists:foldl(fun({{Target, Port}, Options, TTL}, L) ->
			case TTL - 1 of
				TTL2 when TTL2 =< 0 ->
					ok = gen_server:cast(Browse#browse.reader, {service_remove, Domain, Type, {{Target, Port}, Options, TTL2}}),
					L;
				TTL2 ->
					[{{Target, Port}, Options, TTL2} | L]
			end
		end, [], V), D)
	end, dict:new(), Discovered),
	{noreply, Reader#reader{discovered=Discovered2}};
handle_info(Info, Reader) ->
	{ok, Reader2} = handler_info(Info, Reader),
	{noreply, Reader2}.

%% @private
terminate(Reason, Reader) ->
	handler_terminate(Reason, Reader).

%% @private
code_change(_OldVsn, Reader, _Extra) ->
	{ok, Reader}.

%%%-------------------------------------------------------------------
%%% Handler functions
%%%-------------------------------------------------------------------

%% @private
handler_init(Reader=#reader{handler=undefined}) ->
	{ok, Reader};
handler_init(Reader=#reader{handler=Handler, browse=Browse}) ->
	case Handler:browse_init(Browse) of
		{ok, State} ->
			{ok, Reader#reader{state=State}}
	end.

%% @private
handler_add(_Domain, _Type, _Service, Reader=#reader{handler=undefined}) ->
	{ok, Reader};
handler_add(Domain, Type, Service, Reader=#reader{handler=Handler, state=State}) ->
	case Handler:browse_service_add(Domain, Type, Service, State) of
		{ok, State2} ->
			{ok, Reader#reader{state=State2}}
	end.

%% @private
handler_remove(_Domain, _Type, _Service, Reader=#reader{handler=undefined}) ->
	{ok, Reader};
handler_remove(Domain, Type, Service, Reader=#reader{handler=Handler, state=State}) ->
	case Handler:browse_service_remove(Domain, Type, Service, State) of
		{ok, State2} ->
			{ok, Reader#reader{state=State2}}
	end.

%% @private
handler_info(_Info, Reader=#reader{handler=undefined}) ->
	{ok, Reader};
handler_info(Info, Reader=#reader{handler=Handler, state=State}) ->
	case Handler:browse_info(Info, State) of
		{ok, State2} ->
			{ok, Reader#reader{state=State2}}
	end.

%% @private
handler_terminate(_Reason, #reader{handler=undefined}) ->
	ok;
handler_terminate(Reason, #reader{handler=Handler, state=State}) ->
	_ = Handler:browse_terminate(Reason, State),
	ok.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
handle_read([], Reader) ->
	{noreply, Reader};
handle_read([{Key, Val, TTL} | KeyVals], Reader) ->
	{Header, Type, Questions, Answers, Authorities, Resources} = parse_record(Val),
	QR = proplists:get_value(qr, Header),
	Opcode = proplists:get_value(opcode, Header),
	Data = {Questions, Answers, Authorities, Resources},
	case handle_record(Key, Header, Type, QR, Opcode, Data, TTL, Reader) of
		{ok, Reader2} ->
			handle_read(KeyVals, Reader2);
		{stop, Reason, Reader2} ->
			{stop, Reason, Reader2}
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
handle_record(_Key, _Header, msg, true, 'query', {[], Answers, [], Resources}, TTL, Reader) ->
	handle_advertisement(Answers, Resources, TTL, Reader);
handle_record(_Key, _Header, _Type, _QR, _Opcode, _Data, _TTL, Reader) ->
	{ok, Reader}.

%% @private
handle_advertisement([], _Resources, _TTL, Reader) ->
	{ok, Reader};
handle_advertisement([Answer | Answers], Resources, TTL, Reader=#reader{discovered=Discovered, browse=Browse}) ->
	AnswerData = data(Answer),
	AnswerDomain = domain(Answer),
	ResourceTypes = [{type(Resource), data(Resource)} || Resource <- Resources, domain(Resource) =:= AnswerData],
	case get_value(txt, ResourceTypes) of
		undefined ->
			handle_advertisement(Answers, Resources, TTL, Reader);
		TXT ->
			case get_value(srv, ResourceTypes) of
				undefined ->
					handle_advertisement(Answers, Resources, TTL, Reader);
				{_Priority, _Weight, Port, TargetString} ->
					Target = iolist_to_binary(TargetString),
					{Type, Domain} = parse_type_domain(AnswerDomain),
					Options = parse_txt(TXT, []),
					case ttl(Answer, TTL) of
						AnswerTTL when is_integer(AnswerTTL) andalso AnswerTTL =< 0 -> % Remove request
							Service = {{Target, Port}, Options, AnswerTTL},
							Discovered2 = dict:update({Domain, Type}, fun(Services) ->
								lists:keydelete({Target, Port}, 1, Services)
							end, [], Discovered),
							{ok, Reader2} = handler_remove(Domain, Type, Service, Reader#reader{discovered=Discovered2}),
							redis_sd_client_event:service_remove(Domain, Type, Service, Browse),
							handle_advertisement(Answers, Resources, TTL, Reader2);
						AnswerTTL when is_integer(AnswerTTL) -> % Add request
							Service = {{Target, Port}, Options, AnswerTTL},
							Discovered2 = dict:update({Domain, Type}, fun(Services) ->
								lists:keystore({Target, Port}, 1, Services, Service)
							end, [Service], Discovered),
							{ok, Reader2} = handler_add(Domain, Type, Service, Reader#reader{discovered=Discovered2}),
							redis_sd_client_event:service_add(Domain, Type, Service, Browse),
							handle_advertisement(Answers, Resources, TTL, Reader2)
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
parse_txt([], Acc) ->
	lists:reverse(Acc);
parse_txt([TXT | TXTs], Acc) ->
	case binary:split(iolist_to_binary(TXT), <<"=">>) of
		[Key, Val] ->
			parse_txt(TXTs, [{redis_sd:urldecode(Key), redis_sd:urldecode(Val)} | Acc]);
		_ ->
			parse_txt(TXTs, Acc)
	end.
