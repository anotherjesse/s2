-module(req).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([get/1,
         put/3,
         cold/0,
         stop/0,
         start_http/1,
         stop_http/0,
         handle_http/1,
         start_link/0]).

-define(SERVER, global:whereis_name(?MODULE)).

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, {stop}).


% control misultin http server

start_http(Port) ->
    misultin:start_link([{port, Port}, {loop, fun(Req) -> handle_http(Req) end}]).

stop_http() ->
    misultin:stop().

% callback on request received

handle_http(Req) ->
    case Req:get(method) of
        'GET' ->
            io:format("Got a request~n"),
            {abs_path, Uri} = Req:get(uri),
            io:format("Req is for ~s~n", [Uri]),
            case meta:fetch(Uri) of
                not_found ->
                    Req:respond(404, "404 not found");
                Headers ->
                    Req:ok(Headers, storage:fetch(Uri))
            end;
        'PUT' ->
            bucket:insert("bucket", deleteme),
            Req:ok("success");
        _ ->
            io:format("Haven't handled ~p~n", [Req:get(method)]),
            Req:ok("hmm")
    end.


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
    Pid = proc_lib:spawn_link(meta, get, [ObjectId]),
    File = storage:get(ObjectId),
    io:format("Header: ~p~n", [Pid]),
    io:format("Content: ~p~n", [File]),
    {reply, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Msg, State) -> {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    io:format("Reloading code for ?MODULE\n",[]),
    {ok, State}.

cold() ->
    meta:start(),
    storage:start(),
    bucket:start(),
    req:start_link(),
    req:start_http(1234).
