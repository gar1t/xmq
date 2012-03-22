-module(xmq_zmq_context).

-behavior(e2_service).

-export([start_link/0, get_context/0]).

-export([handle_msg/3, terminate/2]).

start_link() ->
    e2_service:start_link(?MODULE, new_context(), [registered]).

get_context() ->
    e2_service:call(?MODULE, get).

handle_msg(get, _From, C) ->
    {reply, C, C}.

terminate(_, C) ->
    ok = erlzmq:term(C, 0).

new_context() ->
    {ok, C} = erlzmq:context(),
    C.
