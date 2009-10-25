-module(req).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([get/1,
         put/3,
         first_run/0,
         start/0,
         stop/0,
         md5_hex/1,
         start_http/1,
         stop_http/0,
         handle_http/1,
         start_link/0]).

-include("s2.hrl").

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
    Method = Req:get(method),
    {abs_path, "/" ++ Uri} = Req:get(uri),
    {match, [{1, BucketLength}]} = regexp:matches(Uri, "^[^/]*"),
    Bucket = string:substr(Uri, 1, BucketLength),
    Key = case string:len(Uri) > BucketLength + 2 of
              true ->
                  string:substr(Uri, BucketLength + 2);
              false ->
                  none
          end,

    io:format("Method: ~p Bucket: ~s Key: ~p~n", [Method, Bucket, Key]),
    handle(Req, {Method, Bucket, Key}).

% note: amazon seems to crop max-keys before computing common prefixes
% fixme: add test for dealing with delimiter being first char of key
handle(Req, {'GET', Bucket, none}) ->
    Prefix = lists:flatten([ Y || {"prefix",Y} <- Req:parse_qs()]),
    Delimiter = lists:flatten([ Y || {"delimiter",Y} <- Req:parse_qs()]),
    MaxKeys = case lists:flatten([ Y || {"max-keys",Y} <- Req:parse_qs()]) of
                  [] ->
                      1000;
                  N ->
                      list_to_integer(N)
              end,
    io:format("Prefix: ~s Delimiter: ~s MaxKeys ~p~n", [Prefix, Delimiter, MaxKeys]),
    case bucket:fetch(Bucket) of
        not_found ->
            Req:respond(404, "No Such Bucket");
        _ ->
            [Keys, CommonPrefixes] = meta:list(Bucket, Prefix, Delimiter),
            KeysXML = [["<Contents><Key>",
                        Obj#object.key,
                        "</Key></Contents>"] || Obj <- lists:sublist(Keys, MaxKeys)],
            CommonPrefixesXML = [["<CommonPrefixes><Prefix>",
                                  CP,
                                  "</Prefix></CommonPrefixes>"] || CP <- CommonPrefixes],
            Req:ok(lists:flatten(["<?xml version='1.0' encoding='UTF-8'?>",
                                  "<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01'>",
                                  "<Name>", Bucket, "</Name>",
                                  "<Prefix>", Prefix, "</Prefix>",
                                  "<Delimiter>", Delimiter, "</Delimiter>",
                                  "<Marker></Marker>",
                                  "<MaxKeys>1000</MaxKeys>",
                                  "<IsTruncated>false</IsTruncated>",
                                  KeysXML,
                                  CommonPrefixesXML,
                                  "</ListBucketResult>"]))
    end;

handle(Req, {'GET', Bucket, Key}) ->
    case meta:fetch(Bucket, Key) of
        not_found ->
            Req:respond(404, "404 not found");
        Headers ->
            Req:ok(Headers, storage:fetch(Bucket, Key))
    end;

handle(Req, {'HEAD', Bucket, Key}) ->
    case meta:fetch(Bucket, Key) of
        not_found ->
            Req:respond(404, "404 not found");
        Headers ->
            Req:ok(Headers, "")
    end;

handle(Req, {'DELETE', Bucket, Key}) ->
    meta:delete(Bucket, Key),
    storage:delete(Bucket, Key),
    Req:respond(204, "");

handle(Req, {'PUT', Bucket, none}) ->
    bucket:insert(Bucket, none),
    Req:ok("success");

handle(Req, {'PUT', Bucket, Key}) ->
    io:format("before~n"),
    Headers = valid_headers(Req:get(headers)),
    io:format("after~n"),
    meta:insert(Bucket, Key, Headers),
    Content = Req:get(body),
    storage:insert(Bucket, Key, Content),
    MD5 = md5_hex(Content),
    Req:ok([{"ETag", "\"" ++ MD5 ++ "\""}], "success");

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

start() ->
    meta:start(),
    storage:start(),
    bucket:start(),
    req:start_link(),
    req:start_http(1234),
    io:format("Setting up on 1234~n").

first_run() ->
    io:format("Building tables~n"),
    meta:first_run(),
    storage:first_run(),
    bucket:first_run().

md5_hex(S) ->
       Md5_bin =  erlang:md5(S),
       Md5_list = binary_to_list(Md5_bin),
       lists:flatten(list_to_hex(Md5_list)).

list_to_hex(L) ->
    lists:map(fun(X) -> int_to_hex(X) end, L).

int_to_hex(N) when N < 256 ->
    [hex(N div 16), hex(N rem 16)].

hex(N) when N < 10 ->
    $0+N;
hex(N) when N >= 10, N < 16 ->
    $a + (N-10).

valid_headers(Headers) ->
    [{K,V} || {K,V} <- Headers, K == 'Content-Type'].
