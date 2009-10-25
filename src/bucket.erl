-module(bucket).
-export([insert/2,
         fetch/1,
         delete/1,
         first_run/0,
         start/0,
         all/0,
         stop/0]).

-include("/usr/lib/erlang/lib/stdlib-1.16.2/include/qlc.hrl").
-include("s2.hrl").

start() ->
    ok = mnesia:start(),
    io:format("Waiting on mnesia tables..\n",[]),
    mnesia:wait_for_tables([bucket], 30000),
    mnesia:table_info(bucket, all),
    ok.

stop() ->
    mnesia:stop().

first_run() ->
    mnesia:create_schema([node()]),
    ok = mnesia:start(),
    mnesia:create_table(bucket,
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

all() ->
    {atomic, Results} = mnesia:transaction(
                          fun() ->
                                  qlc:eval( qlc:q(
                                              [ X || X <- mnesia:table(bucket) ]
                                             ))
                          end ),
    Results.

insert(Bucket, Owner) ->
    Fun = fun() ->
                  mnesia:write(
                    #bucket{ index = Bucket,
                             owner = Owner } )
          end,
    {atomic, Result} = mnesia:transaction(Fun),
    Result.

delete(Bucket) ->
    Delete=#bucket{ index = Bucket, _ = '_'},
    Fun = fun() ->
                  List = mnesia:match_object(Delete),
                  lists:foreach(fun(X) ->
                                        mnesia:delete_object(X)
                                end, List)
          end,
    mnesia:transaction(Fun).
