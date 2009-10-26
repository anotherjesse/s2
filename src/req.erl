-module(req).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([get/1,
         put/3,
         first_run/0,
         start/0,
         stop/0,
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
    case Uri of
        [] ->
            Bucket = none,
            Key = none;
        _ ->
            {match, [{1, BucketLength}]} = regexp:matches(Uri, "^[^/]*"),
            Bucket = string:substr(Uri, 1, BucketLength),
            Key = case string:len(Uri) > BucketLength + 2 of
                      true ->
                          string:substr(Uri, BucketLength + 2);
                      false ->
                          none
                  end
    end,
    io:format("Method: ~s Bucket: ~s Key: ~p~n", [Method, Bucket, Key]),
    handle(Req, {Method, Bucket, Key}).

handle(Req, {'GET', none, none}) ->
    Req:ok([{'Content-Type', 'text/xml'}],
           lists:flatten(["<ListAllMyBucketsResult xmlns='http://doc.s3.amazonaws.com/2006-03-01'>",
                          "<Owner><ID>foo</ID><DisplayName>bar</DisplayName></Owner>",
                          "<Buckets>",
                           [["<Bucket>",
                             "<Name>", Obj#bucket.index, "</Name>"
                             "<CreationDate>2006-02-03T16:45:09.000Z</CreationDate>",
                             "</Bucket>"] || Obj <- bucket:all()],
                          "</Buckets></ListAllMyBucketsResult>"]));


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
            KeysXML = [["<Contents>",
                        "<Key>", Obj#object.key, "</Key>",
                        "<LastModified>", date_fmt(Obj#object.last_modified), "</LastModified>",
                        "<ETag>&quot;", Obj#object.etag, "&quot;</ETag>",
                        "<Size>", Obj#object.size, "</Size>",
                        "</Contents>"] || Obj <- lists:sublist(Keys, MaxKeys)],
            CommonPrefixesXML = [["<CommonPrefixes><Prefix>",
                                  CP,
                                  "</Prefix></CommonPrefixes>"] || CP <- CommonPrefixes],
            Req:ok([{"Content-Type", "text/xml"}],
                   lists:flatten(["<?xml version='1.0' encoding='UTF-8'?>",
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
            Req:respond(404, "");
        Headers ->
            Req:file_send(storage:fetch(Bucket, Key), Headers)
    end;

handle(Req, {'HEAD', Bucket, Key}) ->
    case meta:fetch(Bucket, Key) of
        not_found ->
            Req:respond(404, "");
        Headers ->
            Req:ok(Headers, "")
    end;

handle(Req, {'DELETE', Bucket, none}) ->
    bucket:delete(Bucket),
    Req:respond(204, "");

handle(Req, {'DELETE', Bucket, Key}) ->
    meta:delete(Bucket, Key),
    storage:delete(Bucket, Key),
    Req:respond(204, "");

handle(Req, {'PUT', Bucket, none}) ->
    bucket:insert(Bucket, none),
    Req:ok("success");

handle(Req, {'PUT', Bucket, Key}) ->
    meta:insert(Bucket, Key, Req),
    Content = Req:get(body),
    storage:insert(Bucket, Key, Content),
    Req:ok([{"ETag", "\"" ++ md5:hex_digest(Content) ++ "\""}], "success");

handle(Req, {Method, Bucket, Key}) ->
    Req:respond(501, io_lib:format("Haven't handled ~p ~p ~p~n", [Method, Bucket, Key])).

get(Object) ->
    gen_server:call(?SERVER, {get, Object}, infinity).

put(Object, Headers, Content) ->
    gen_server:call(?SERVER, {put, Object, Headers, Content}, infinity).

init([]) ->
    {ok, []}.

handle_call({stop}, _From, State) ->
    {stop, stop, State}.


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
    io:format("Listening on 1234~n").

first_run() ->
    io:format("Building tables~n"),
    meta:first_run(),
    storage:first_run(),
    bucket:first_run().


date_fmt(DateTime) ->
    {{Year,Month,Day},{Hour,Min,Sec}} = DateTime,
    io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B.000Z",
        [Year, Month, Day, Hour, Min, Sec]).
