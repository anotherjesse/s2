-module(storage).
-export([insert/3,
         fetch/2,
         delete/2,
         first_run/0,
         start/0,
         stop/0]).

-record(file, {index, content}).

start() ->
    ok = mnesia:start(),
    io:format("Waiting on mnesia tables..\n",[]),
    mnesia:wait_for_tables([file], 30000),
    mnesia:table_info(file, all),
    ok.

stop() ->
    mnesia:stop().

first_run() ->
    mnesia:create_schema([node()]),
    ok = mnesia:start(),
    mnesia:create_table(file,
                        [ {disc_copies, [node()] },
                          {attributes,
                           record_info(fields,file)} ]).
fetch(Bucket, Key) ->
    Id = Bucket ++ "/" ++ Key,
    Fun = fun() ->
                  mnesia:read({file, Id})
          end,
    case mnesia:transaction(Fun) of
        {atomic, []} ->
            not_found;
        {atomic, [File]} ->
            File#file.content
    end.

insert(Bucket, Key, Content) ->
    Id = Bucket ++ "/" ++ Key,
    Fun = fun() ->
                  mnesia:write(
                    #file{ index   = Id,
                           content = Content } )
          end,
    {atomic, Result} = mnesia:transaction(Fun),
    Result.

delete(Bucket, Key) ->
    Id = Bucket ++ "/" ++ Key,
    Delete=#file{ index = Id, _ = '_'},
    Fun = fun() ->
                  List = mnesia:match_object(Delete),
                  lists:foreach(fun(X) ->
                                        mnesia:delete_object(X)
                                end, List)
          end,
    mnesia:transaction(Fun).
