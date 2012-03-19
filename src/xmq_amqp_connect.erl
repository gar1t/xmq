-module(xmq_amqp_connect).

-behavior(e2_task).

-export([start_link/1, start_link/2]).

-export([handle_task/1]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Params) ->
    start_link(Params, []).

start_link(Params, Options) ->
    e2_task:start_link(?MODULE, [Params, Options, self()]).

%%%===================================================================
%%% Task callbacks
%%%===================================================================

handle_task([Params, Options, Parent]) ->
    handle_connect_result(connect(Params), Options, Parent).

%%%===================================================================
%%% Internal functions
%%%===================================================================

connect(Params) ->
    amqp_connection:start(Params).

handle_connect_result({ok, Connection}, Options, Parent) ->
    notify_connect(Connection, notify_target(Options, Parent)),
    {stop, normal}.

notify_target(Options, Parent) ->
    proplists:get_value(notify, Options, Parent).

notify_connect(Connection, Pid) when is_pid(Pid) ->
    erlang:send(Pid, {amqp_connect, Connection});
notify_connect(Connection, Fun) when is_function(Fun) ->
    Fun(Connection).
