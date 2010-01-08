%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Callbacks for the s2 application.

-module(s2_app).
-author('author <author@example.com>').

-behaviour(application).
-export([start/2, stop/1]).


%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for s2.
start(_Type, _StartArgs) ->
    s2_deps:ensure(),
    s2_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for s2.
stop(_State) ->
    ok.


%%
%% Tests
%%
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).
-endif.
