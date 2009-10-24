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
    io:format("Got a request~n"),
    {abs_path, "/" ++ Uri} = Req:get(uri),
    {match, [{1, BucketLength}]} = regexp:matches(Uri, "^[^/]*"),
    Bucket = string:substr(Uri, 1, BucketLength),
    Key = case string:len(Uri) > BucketLength + 2 of
              true ->
                  string:substr(Uri, BucketLength + 2);
              false ->
                  none
          end,
    io:format("Bucket: ~s Key: ~p~n", [Bucket, Key]),
    handle(Req, {Req:get(method), Bucket, Key}).


handle(Req, {'GET', _Bucket, none}) ->
    Req:respond(501, "Handle bucket request");

handle(Req, {'GET', Bucket, Key}) ->
    case meta:fetch(Bucket, Key) of
        not_found ->
            Req:respond(404, "404 not found");
        Headers ->
            Req:send_object(Req, Headers, storage:fetch(Bucket, Key))
    end;

handle(Req, {'PUT', Bucket, none}) ->
    bucket:insert(Bucket, none),
    Req:ok("success");

handle(Req, {Method, Bucket, Key}) ->
    Req:respond(501, io_lib:format("Haven't handled ~p ~p ~p~n", [Method, Bucket, Key])).

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
