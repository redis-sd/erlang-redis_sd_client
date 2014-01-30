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
-module(redis_sd_client).
-behaviour(gen_server).

-include("redis_sd_client.hrl").

-define(SERVER, ?MODULE).
-define(TAB, ?MODULE).

%% API
-export([manual_start/0, start_link/0]).
-export([enable/0, enable/1, disable/0, disable/1, list/0,
	list_browses/0, list_records/0, list_records/1]).
-export([new_browse/1, rm_browse/1, delete_browse/1, set_enabled/2]).

%% Name Server API
-export([register_name/2, whereis_name/1, unregister_name/1, send/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	terminate/2, code_change/3]).

-type monitors() :: [{{reference(), pid()}, any()}].
-record(state, {
	monitors = [] :: monitors()
}).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Manually start redis_sd_client and all dependencies.
-spec manual_start() -> ok.
manual_start() ->
	redis_sd:require([
		backoff,
		hierdis,
		redis_sd_spec,
		redis_sd_client
	]).

%% @private
start_link() ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Enables all browses.
enable() ->
	enable([Pid || {_Name, Pid} <- list()]).

%% @doc Enables the list of browse refs or pids.
enable(Browses) when is_list(Browses) ->
	_ = [redis_sd_client_browse:enable(B) || B <- Browses],
	ok.

%% @doc Disables all browses.
disable() ->
	disable([Pid || {_Name, Pid} <- list()]).

%% @doc Disables the list of browse refs or pids.
disable(Browses) when is_list(Browses) ->
	_ = [redis_sd_client_browse:disable(B) || B <- Browses],
	ok.

%% @doc List all browse refs and their pids.
list() ->
	ets:select(?TAB, [{{{pid, '$1'}, '$2'}, [], [{{'$1', '$2'}}]}]).

%% @doc List all browses in the format of: {Ref, {Enabled, Pid, StatePid, Records}}
list_browses() ->
	FoldFun = fun
		({{enabled, Ref}, Enabled}, Acc) ->
			UpdateFun = fun({_, Pid, StatePid, Records}) ->
				{Enabled, Pid, StatePid, Records}
			end,
			orddict:update(Ref, UpdateFun, {Enabled, undefined, undefined, []}, Acc);
		({{pid, Ref}, Pid}, Acc) ->
			UpdateFun = fun({Enabled, _, StatePid, Records}) ->
				{Enabled, Pid, StatePid, Records}
			end,
			orddict:update(Ref, UpdateFun, {undefined, Pid, undefined, []}, Acc);
		({{state_pid, Ref}, StatePid}, Acc) ->
			UpdateFun = fun({Enabled, Pid, _, Records}) ->
				{Enabled, Pid, StatePid, Records}
			end,
			orddict:update(Ref, UpdateFun, {undefined, undefined, StatePid, []}, Acc);
		({{record, Ref, Key}, Record}, Acc) ->
			UpdateFun = fun({Enabled, Pid, StatePid, Records}) ->
				{Enabled, Pid, StatePid, [{Key, Record} | Records]}
			end,
			orddict:update(Ref, UpdateFun, {undefined, undefined, undefined, [{Key, Record}]}, Acc);
		(_, Acc) ->
			Acc
	end,
	ets:foldl(FoldFun, orddict:new(), ?TAB).

list_records() ->
	list_records('_').

list_records(BrowseRef) ->
	ets:select(?TAB, [{{{record, BrowseRef, '$1'}, '$2'}, [], [{{'$1', '$2'}}]}]).

new_browse(BrowseConfig) ->
	redis_sd_client_sup:new_browse(BrowseConfig).

%% @doc Gracefully terminate the named browse.
rm_browse(BrowseName) ->
	redis_sd_client_sup:rm_browse(BrowseName).

%% @doc Forcefully terminate the named browse.
delete_browse(BrowseName) ->
	redis_sd_client_sup:delete_browse(BrowseName).

%% @private
set_enabled(BrowseRef, Enabled) ->
	case is_browse_owner(BrowseRef) of
		false ->
			false;
		true ->
			gen_server:call(?SERVER, {set_enabled, BrowseRef, Enabled})
	end.

%%%===================================================================
%%% Name Server API functions
%%%===================================================================

-spec register_name(Name::term(), Pid::pid()) -> 'yes' | 'no'.
register_name(Name, Pid) when is_pid(Pid) ->
	gen_server:call(?SERVER, {register_name, Name, Pid}, infinity).

-spec whereis_name(Name::term()) -> pid() | 'undefined'.
whereis_name(Pid) when is_pid(Pid) ->
	Pid;
whereis_name({Atom, Pid}) when is_atom(Atom) andalso is_pid(Pid) ->
	Pid;
whereis_name(Name) ->
	case ets:lookup(?TAB, Name) of
		[{Name, Pid}] ->
			case erlang:is_process_alive(Pid) of
				true ->
					Pid;
				false ->
					undefined
			end;
		[] ->
			undefined
	end.

-spec unregister_name(Name::term()) -> term().
unregister_name(Name) ->
	case whereis_name(Name) of
		undefined ->
			ok;
		_ ->
			_ = ets:delete(?TAB, Name),
			ok
	end.

-spec send(Name::term(), Msg::term()) -> Pid::pid().
send(Name, Msg) ->
	case whereis_name(Name) of
		Pid when is_pid(Pid) ->
			Pid ! Msg,
			Pid;
		undefined ->
			erlang:error(badarg, [Name, Msg])
	end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
	Monitors = [{{erlang:monitor(process, Pid), Pid}, Ref} || [Ref, Pid] <- ets:match(?TAB, {{pid, '$1'}, '$2'})],
	ok = redis_sd_client_event:add_handler(redis_sd_event_handler, self()),
	{ok, #state{monitors=Monitors}}.

%% @private
handle_call({register_name, Name, Pid}, _From, State=#state{monitors=Monitors}) ->
	case ets:insert_new(?TAB, {Name, Pid}) of
		true ->
			MonitorRef = erlang:monitor(process, Pid),
			{reply, yes, State#state{monitors=[{{MonitorRef, Pid}, Name} | Monitors]}};
		false ->
			{reply, no, State}
	end;
handle_call({set_enabled, Ref, Enabled}, _From, State) ->
	Reply = ets:insert(?TAB, {{enabled, Ref}, Enabled}),
	{reply, Reply, State};
handle_call(_Request, _From, State) ->
	{reply, ignore, State}.

%% @private
handle_cast(_Request, State) ->
	{noreply, State}.

%% @private
handle_info({'$redis_sd', {browse, terminate, normal, ?REDIS_SD_BROWSE{ref=Ref}}}, State) ->
	ok = redis_sd_client:delete_browse(Ref),
	{noreply, State};
handle_info({'$redis_sd', {record, add, Key, Record, _Browse=?REDIS_SD_BROWSE{ref=Ref}}}, State) ->
	true = ets:insert(?TAB, {{record, Ref, Key}, Record}),
	{noreply, State};
handle_info({'$redis_sd', {record, expire, Key, _Record, _Browse=?REDIS_SD_BROWSE{ref=Ref}}}, State) ->
	true = ets:delete(?TAB, {record, Ref, Key}),
	{noreply, State};
handle_info({'$redis_sd', _Event}, State) ->
	{noreply, State};
handle_info({'DOWN', MonitorRef, process, Pid, _Reason}, State=#state{monitors=Monitors}) ->
	case lists:keytake({MonitorRef, Pid}, 1, Monitors) of
		{value, {{MonitorRef, Pid}, {pid, Ref}}, Monitors2} ->
			true = ets:delete(?TAB, {pid, Ref}),
			true = ets:delete(?TAB, {enabled, Ref}),
			true = ets:match_delete(?TAB, {{record, Ref, '_'}, '_'}),
			{noreply, State#state{monitors=Monitors2}};
		{value, {{MonitorRef, Pid}, {state_pid, Ref}}, Monitors2} ->
			true = ets:delete(?TAB, {state_pid, Ref}),
			{noreply, State#state{monitors=Monitors2}};
		false ->
			{noreply, State}
	end;
handle_info(Info, State) ->
	error_logger:error_msg(
		"** ~p ~p unhandled info in ~p/~p~n"
		"   Info was: ~p~n",
		[?MODULE, self(), handle_info, 2, Info]),
	{noreply, State}.

%% @private
terminate(_Reason, _State) ->
	ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
is_browse_owner(BrowseRef) ->
	Self = self(),
	try whereis_name({pid, BrowseRef}) of
		Self ->
			true;
		_ ->
			false
	catch
		_:_ ->
			false
	end.
