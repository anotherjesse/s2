-module(meta).
-export([insert/3,
         fetch/1,
         fetch/2,
         delete/2,
         first_run/0,
         start/0,
         stop/0]).

-include("s2.hrl").

start() ->
    ok = mnesia:start(),
    io:format("Waiting on mnesia tables..\n",[]),
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
            mnesia:match_object({object, '_', Bucket, '_', '_' } )
        end,
    {atomic, Results} = mnesia:transaction( Fun),
    Results.


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

insert(Bucket, Key, Headers) ->
    Id = Bucket ++ "/" ++ Key,
    Fun = fun() ->
                  mnesia:write(
                    #object{ index   = Id,
                             bucket  = Bucket,
                             key     = Key,
                             headers = Headers } )
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