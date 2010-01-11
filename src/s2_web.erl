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
    % Response = Req:ok({"text/html; charset=utf-8",
    %                   [{"Server","Mochiweb-Test"}],
    %                   chunked}),
    % Response:write_chunk("testing 1 2 3"),
    % Response:write_chunk("").
    handle(Req, {Method, Bucket, Key}).

handle(Req, {'GET', none, none}) ->
    Req:ok({'text/xml',
            lists:flatten(["<ListAllMyBucketsResult xmlns='http://doc.s3.amazonaws.com/2006-03-01'>",
                           "<Owner><ID>foo</ID><DisplayName>bar</DisplayName></Owner>",
                           "<Buckets>",
                            [["<Bucket>",
                              "<Name>", Domain, "</Name>"
                              "<CreationDate>2006-02-03T16:45:09.000Z</CreationDate>",
                              "</Bucket>"] || Domain <- mogilefs:get_domains()],
                           "</Buckets></ListAllMyBucketsResult>"])});


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
            Req:not_found();
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
            Req:ok({"text/xml",
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
                                   "</ListBucketResult>"])})
    end;

handle(Req, {'GET', Bucket, Key}) ->
    case s2_meta:fetch(Bucket, Key) of
        not_found ->
            Req:respond(404, "");
        Headers ->
            { ok, Paths } = mogilefs:get_paths(Bucket, Key),
            { ok, _Status, Body} = http:request(proplists:get_value("path1", Paths)),
            % FIXME: mimetype?
            Response = Req:ok({"text/html; charset=utf-8",
                              [{"Server","Mochiweb-Test"}],
                              chunked}),
            Response:write_chunk(Body),
            Response:write_chunk("")
    end;

handle(Req, {'HEAD', Bucket, Key}) ->
    case s2_meta:fetch(Bucket, Key) of
        not_found ->
            Req:not_found();
        Headers ->
            Req:ok({Headers, ""})
    end;

handle(Req, {'DELETE', Bucket, none}) ->
    mogilefs:delete_domain(Bucket),
    Req:respond({204, [{"Content-Type", "text/xml"}], "<result>ok</result>"});

handle(Req, {'DELETE', Bucket, Key}) ->
    s2_meta:delete(Bucket, Key),
    mogilefs:delete(Bucket, Key),
    Req:respond({204, [{"Content-Type", "text/xml"}], "<result>ok</result>"});

handle(Req, {'PUT', Bucket, none}) ->
    {ok, _} = mogilefs:create_domain(Bucket),
    Req:ok({"text/xml", "<result>ok</result>"});

handle(Req, {'PUT', Bucket, Key}) ->
    io:format("line 1~n"),
    Content = Req:recv_body(),
    Etag = md5:hex_digest(Content),
    Headers = Req:get(headers),
    Size = Req:get_header_value("content-length"),
    s2_meta:insert(Bucket, Key, Etag, Headers, Size),
    io:format("line 2~n"),
    io:format("Upload size: ~p~n", [Size]),
    io:format("line 4~n"),
    {ok, Create} = mogilefs:create_open(Bucket, Key),
    io:format("line 5~n"),
    Path = proplists:get_value("path", Create),
    io:format("line 6~n"),
    io:format("line 7~n"),
    Result = http:request(put, {Path, [], "text/plain", Content}, [], []),
    io:format("line 8~n"),
    {ok, _} = mogilefs:create_close(Bucket, Key, Create, Size),
    io:format("line 9~n"),
    Req:ok({"text/xml", [{"ETag", "\"" ++ Etag ++ "\""}], "<result>success</result>"});

handle(Req, {Method, Bucket, Key}) ->
    Req:respond({501, "text/xml", io_lib:format("<result>Haven't handled ~p ~p ~p</result>", [Method, Bucket, Key])}).

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
