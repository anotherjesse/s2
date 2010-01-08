%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc TEMPLATE.

-module(s2).
-author('author <author@example.com>').
-export([start/0, stop/0]).

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.

%% @spec start() -> ok
%% @doc Start the s2 server.
start() ->
    s2_deps:ensure(),
    ensure_started(crypto),
    application:start(s2).

%% @spec stop() -> ok
%% @doc Stop the s2 server.
stop() ->
    Res = application:stop(s2),
    application:stop(crypto),
    Res.
