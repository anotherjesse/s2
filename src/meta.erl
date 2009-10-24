-module(meta).
-export([insert/3,
         fetch/2,
         first_run/0,
         start/0,
         stop/0]).

-record(object, {index, headers}).

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
                             headers = Headers } )
          end,
    {atomic, Result} = mnesia:transaction(Fun),
    Result.
