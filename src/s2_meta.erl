%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Metadata server for s2.

-module(s2_meta).
-author('Jesse Andrews <jesse@ang.st>').

-export([insert/3,
         list/3,
         fetch/1,
         fetch/2,
         delete/2,
         first_run/0,
         start/0,
         stop/0]).

-include("s2.hrl"). 

start() ->
    ok = mnesia:start(),
    mnesia:wait_for_tables([object], 30000),
    mnesia:table_info(object, all),
    ok.

stop() ->
    mnesia:stop().

first_run() ->
    mnesia:create_schema([node()]),
    ok = mnesia:start(),
    mnesia:create_table(object,
                        [ {disc_copies, [node()] },
                          {attributes,
                           record_info(fields,object)} ]).

fetch(Bucket) ->
    Fun =
        fun() ->
            mnesia:match_object({object, '_', Bucket, '_', '_', '_', '_', '_' } )
        end,
    {atomic, Results} = mnesia:transaction(Fun),
    Results.

list(Bucket, [], []) ->
    [fetch(Bucket), []];

list(Bucket, [], Delimiter) ->
    extract_common_prefixes(hd(list(Bucket, [], [])), Delimiter);

list(Bucket, Prefix, []) ->
    [[Obj || Obj <- fetch(Bucket), string:str(Obj#object.key, Prefix) == 1 ],
     []];

list(Bucket, Prefix, Delimiter) ->
    extract_common_prefixes(hd(list(Bucket, Prefix, [])), Delimiter).

extract_common_prefixes(Keys, Delimiter) ->
    {NoPrefix, Prefixed} = lists:partition(fun(A) -> string:str(A#object.key, Delimiter) == 0 end, Keys),
    [NoPrefix,
     lists:usort([hd(string:tokens(Key#object.key, Delimiter)) ++ Delimiter || Key <- Prefixed])].

fetch(Bucket, Key) ->
    Id = Bucket ++ "/" ++ Key,
    Fun =
        fun() ->
                mnesia:read({object, Id})
        end,
    case mnesia:transaction(Fun) of
        {atomic, []} ->
            not_found;
        {atomic, [Object]} ->
            Object#object.headers
    end.

insert(Bucket, Key, Req) ->
    Id = Bucket ++ "/" ++ Key,
    Fun = fun() ->
                  mnesia:write(
                    #object{ index         = Id,
                             bucket        = Bucket,
                             key           = Key,
                             etag          = md5:hex_digest(Req:get(body)),
                             size          = Req:get(content_length),
                             last_modified = calendar:universal_time(),
                             headers       = lists:flatten([extract(K,V) || {K,V} <- Req:get(headers)])
                            })
          end,
    {atomic, Result} = mnesia:transaction(Fun),
    Result.

delete(Bucket, Key) ->
    Id = Bucket ++ "/" ++ Key,
    Delete=#object{ index = Id, _ = '_'},
    Fun = fun() ->
                  List = mnesia:match_object(Delete),
                  lists:foreach(fun(X) ->
                                        mnesia:delete_object(X)
                                end, List)
          end,
    mnesia:transaction(Fun).

%% Extract only headers we care about

% FIXME: misultin messes with header names

extract('Content-Type', V) ->
    {'Content-Type', V};

extract('Date', V) ->
    {'Date', V};

extract('Content-Md5', V) ->
    {'Content-Md5', V};

extract("X-Amz-Meta-" ++ K, V) ->
    {"x-amz-meta-" ++ K, V};

extract(_,_) ->
    [].



%%
%% Tests
%%
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).
-endif.
