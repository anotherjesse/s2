-module(storage).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([get/1,
         put/2,
         stop/0,
         first_run/0,
         start_link/0]).

-record(file, {index, content}).
-record(state, {}). % state is all in mnesia
-define(SERVER, global:whereis_name(?MODULE)).

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, {stop}).

get(Object) ->
    gen_server:call(?SERVER, {get, Object}, infinity).

put(Object, Content) ->
    gen_server:call(?SERVER, {put, Object, Content}, infinity).

init([]) ->
    ok = mnesia:start(),
    io:format("Waiting on mnesia tables..\n",[]),
    mnesia:wait_for_tables([file], 30000),
    Info = mnesia:table_info(file, all),
    io:format("OK. Object table info: \n~w\n\n",[Info]),
    {ok, #state{}}.

handle_call({stop}, _From, State) ->
    {stop, stop, State};

handle_call({put, Object, Content}, _From, State) ->
    Fun = fun() ->
                  mnesia:write(
                    #file{ index   = Object,
                           content = Content } )
          end,
    {atomic, Result} = mnesia:transaction(Fun),
    {reply, Result, State};

handle_call({get, Object}, _From, State) ->
    Fun =
        fun() ->
                mnesia:read({object, Object})
        end,
    {atomic, [Row]} = mnesia:transaction(Fun),
    {object, Object, Content} = Row,
    {reply, Content, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Msg, State) -> {noreply, State}.

terminate(_Reason, _State) ->
    mnesia:stop(),
    ok.

code_change(_OldVersion, State, _Extra) ->
    io:format("Reloading code for ?MODULE\n",[]),
    {ok, State}.

first_run() ->
    mnesia:create_schema([node()]),
    ok = mnesia:start(),
    mnesia:create_table(file,
                        [ {disc_copies, [node()] },
                          {attributes,
                           record_info(fields,file)} ]).
