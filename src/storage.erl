-module(storage).
-export([insert/3,
         fetch/2,
         delete/2,
         first_run/0,
         start/0,
         stop/0]).

start() ->
    ok.

stop() ->
    ok.

first_run() ->
    ok.

fetch(Bucket, Key) ->
    Id = Bucket ++ "/" ++ Key,
    md5:hex_digest(Id).

insert(Bucket, Key, Content) ->
    Id = Bucket ++ "/" ++ Key,
    file:write_file(md5:hex_digest(Id), Content).

delete(Bucket, Key) ->
    Id = Bucket ++ "/" ++ Key,
    file:delete(md5:hex_digest(Id)).

