%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Web server for s2.

-module(s2_web).
-author('Jesse Andrews <jesse@ang.st>').

-export([start/1, stop/0, loop/2]).

-include("s2.hrl"). 

%% External API

start(Options) ->
    % FIXME: where should the meatdata server be started?
    s2_meta:start(),
    {DocRoot, Options1} = get_option(docroot, Options),
    Loop = fun (Req) ->
                   ?MODULE:loop(Req, DocRoot)
           end,
    mochiweb_http:start([{name, ?MODULE}, {loop, Loop} | Options1]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req, _DocRoot) ->
    Method = Req:get(method),
    "/" ++ Path = Req:get(path),
    case Path of
        [] ->
            Bucket = none,
            Key = none;
        _ ->
            {match, [{0, BucketLength}]} = re:run(Path, "^[^/]*"),
            Bucket = string:substr(Path, 1, BucketLength),
            Key = case string:len(Path) > BucketLength + 2 of
                      true ->
                          string:substr(Path, BucketLength + 2);
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
                             "<Name>", Domain, "</Name>"
                             "<CreationDate>2006-02-03T16:45:09.000Z</CreationDate>",
                             "</Bucket>"] || Domain <- mogilefs:get_domains()],
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
    case mogilefs:check_domain(Bucket) of
        not_found ->
            Req:respond(404, "No Such Bucket");
        _ ->
            [Keys, CommonPrefixes] = s2_meta:list(Bucket, Prefix, Delimiter),
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
    case s2_meta:fetch(Bucket, Key) of
        not_found ->
            Req:respond(404, "");
        Headers ->
            { ok, Paths } = mogilefs:get_paths(Bucket, Key),
            { ok, {_Status, _Headers, Body }} = http:request(proplists:get_value("path1", Paths)),
            Req:start_response({200, Headers}),
            Req:send(Body),
            Req:stream(close)
    end;

handle(Req, {'HEAD', Bucket, Key}) ->
    case s2_meta:fetch(Bucket, Key) of
        not_found ->
            Req:respond(404, "");
        Headers ->
            Req:ok(Headers, "")
    end;

handle(Req, {'DELETE', Bucket, none}) ->
    mogilefs:delete_domain(Bucket),
    Req:respond(204, "");

handle(Req, {'DELETE', Bucket, Key}) ->
    s2_meta:delete(Bucket, Key),
    mogilefs:delete(Bucket, Key),
    Req:respond(204, "");

handle(Req, {'PUT', Bucket, none}) ->
    {ok, _} = mogilefs:create_domain(Bucket),
    Req:ok("success");

handle(Req, {'PUT', Bucket, Key}) ->
    s2_meta:insert(Bucket, Key, Req),
    Size = proplists:get_value('Content-Length', Req:get(headers)),
    io:format("Upload size: ~p~n", [Size]),
    {ok, Create} = mogilefs:create_open(Bucket, Key),
    Path = proplists:get_value("path", Create),
    % FIXME: Content should be read/sent via streaming...
    Content = Req:recv_body(),
    Result = http:request(put, {Path, [], "text/plain", Content)}, [], []),
    {ok, _} = mogilefs:create_close(Bucket, Key, Create, Size),
    % FIXME: how do we get the ETAG then?
    Req:ok([{"ETag", "\"" ++ md5:hex_digest(Content) ++ "\""}], "success");

handle(Req, {Method, Bucket, Key}) ->
    Req:respond(501, io_lib:format("Haven't handled ~p ~p ~p~n", [Method, Bucket, Key])).

%% Internal API

get_option(Option, Options) ->
    {proplists:get_value(Option, Options), proplists:delete(Option, Options)}.

date_fmt(DateTime) ->
    {{Year,Month,Day},{Hour,Min,Sec}} = DateTime,
    io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B.000Z",
        [Year, Month, Day, Hour, Min, Sec]).


%%
%% Tests
%%
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).
-endif.
