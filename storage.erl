-module(storage).
-export([insert/2,
         fetch/1,
         first_run/0,
         start/0,
         stop/0]).

-record(file, {index, content}).

start() ->
    ok = mnesia:start(),
    io:format("Waiting on mnesia tables..\n",[]),
    mnesia:wait_for_tables([files], 30000),
    mnesia:table_info(files, all),
    ok.

stop() ->
    mnesia:stop().

first_run() ->
    mnesia:create_schema([node()]),
    ok = mnesia:start(),
    mnesia:create_table(files,
                        [ {disc_copies, [node()] },
                          {attributes,
                           record_info(fields,file)} ]).
fetch(Id) ->
    Fun =
        fun() ->
                mnesia:read({file, Id})
        end,
    case mnesia:transaction(Fun) of
        {atomic, []} ->
            not_found;
        {atomic, [File]} ->
            File#file.content
    end.

insert(Id, Content) ->
    Fun = fun() ->
                  mnesia:write(
                    #file{ index   = Id,
                           content = Content } )
          end,
    {atomic, Result} = mnesia:transaction(Fun),
    Result.
