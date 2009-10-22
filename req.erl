-module(req).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([get/1,
         put/3,
         stop/0,
         start_link/0]).

-define(SERVER, global:whereis_name(?MODULE)).

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, {stop}).

get(Object) ->
    gen_server:call(?SERVER, {get, Object}, infinity).

put(Object, Headers, Content) ->
    gen_server:call(?SERVER, {put, Object, Headers, Content}, infinity).

init([]) ->
    {ok, []}.

handle_call({stop}, _From, State) ->
    {stop, stop, State};

handle_call({put, ObjectId, Headers, Content}, _From, State) ->
    ok = meta:put(ObjectId, Headers),
    ok = storage:put(ObjectId, Content),
    {reply, ok, State};

handle_call({get, ObjectId}, _From, State) ->
    Headers = meta:get(ObjectId),
    File = storage:get(ObjectId),
    io:format("Header: ~p~n", [Headers]),
    io:format("Content: ~p~n", [File]),
    {reply, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Msg, State) -> {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    io:format("Reloading code for ?MODULE\n",[]),
    {ok, State}.
