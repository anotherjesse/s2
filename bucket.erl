-module(bucket).
-export([insert/2,
         fetch/1,
         first_run/0,
         start/0,
         stop/0]).

-record(bucket, {index, owner}).

start() ->
    ok = mnesia:start(),
    io:format("Waiting on mnesia tables..\n",[]),
    mnesia:wait_for_tables([buckets], 30000),
    Info = mnesia:table_info(buckets, all),
    io:format("OK. Buckets table info: \n~w\n\n", [Info]),
    ok.

stop() ->
    mnesia:stop().

first_run() ->
    mnesia:create_schema([node()]),
    ok = mnesia:start(),
    mnesia:create_table(buckets,
                        [ {disc_copies, [node()] },
                          {attributes,
                           record_info(fields,bucket)} ]).
fetch(Id) ->
    Fun =
        fun() ->
                mnesia:read({bucket, Id})
        end,
    case mnesia:transaction(Fun) of
        {atomic, []} ->
            not_found;
        {atomic, [Bucket]} ->
            Bucket#bucket.owner
    end.

insert(Bucket, Owner) ->
    Fun = fun() ->
                  mnesia:write(
                    #bucket{ index = Bucket,
                             owner = Owner } )
          end,
    {atomic, Result} = mnesia:transaction(Fun),
    Result.