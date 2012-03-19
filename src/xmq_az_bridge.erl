-module(xmq_az_bridge).

-include_lib("amqp_client/include/amqp_client.hrl").

-behavior(e2_service).

-export([start_link/2]).

-export([init/1, handle_msg/3, terminate/2]).

-record(state, {bindings, aconn, zcontext, zrouter, routes}).

-define(DEFAULT_AMQP_PARAMS, #amqp_params_network{}).
-define(DEFAULT_ROUTER_ENDPOINT, "tcp://*:5555").

-define(ZCONTEXT_TERM_TIMEOUT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Bindings, Options) ->
    {ServiceOpts, BridgeOpts} =
        e2_service_impl:split_options(?MODULE, Options),
    e2_service:start_link(?MODULE, [Bindings, BridgeOpts], ServiceOpts).

%%%===================================================================
%%% Service callbacks
%%%===================================================================

init([Bindings, Options]) ->
    start_amqp_connect(Options),
    ZmqContext = init_zmq_router(Options),
    {ok, init_state(Bindings, ZmqContext)}.

handle_msg({amqp_connect, Connection}, _From, State) ->
    handle_amqp_connect(Connection, State);
handle_msg({zmq_bind, Router}, _From, State) ->
    handle_zmq_router(Router, State);
handle_msg({'EXIT', _Pid, normal}, _From, State) ->
    %% Linked tasks used for connections
    {noreply, State};
handle_msg({'basic.consume_ok', _CTag}, _From, State) ->
    %% Ack for queue subscription
    {noreply, State};
handle_msg(Msg, _From, State) ->
    e2_log:info({unhandled_az_bridge_msg, Msg}),
    {noreply, State}.

terminate(_Reason, State) ->
    close_zmq_router(State),
    term_zmq_context(State),
    close_amqp_connection(State).

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_state(Bindings, ZmqContext) ->
    #state{bindings=Bindings,
           zcontext=ZmqContext,
           routes=new_routes()}.

init_zmq_router(Options) ->
    {ok, Context} = erlzmq:context(),
    {ok, Router} = erlzmq:socket(Context, [router, {active, true}]),
    ok = erlzmq:bind(Router, router_endpoint(Options)),
    Context.

new_routes() ->
    {ok, Routes} = xmq_routes:start_link(),
    Routes.

start_amqp_connect(Options) ->
    xmq_amqp_connect:start_link(amqp_params(Options)).

amqp_params(Options) ->
    proplists:get_value(amqp_params, Options, ?DEFAULT_AMQP_PARAMS).

router_endpoint(Options) ->
    proplists:get_value(router_endpoint, Options, ?DEFAULT_ROUTER_ENDPOINT).

handle_amqp_connect(Connection, #state{bindings=Bindings}=State) ->
    e2_log:info(az_bridge_amqp_connected),
    setup_amqp_queue(Connection, Bindings),
    {noreply, State#state{aconn=Connection}}.

handle_zmq_router(Router, State) ->
    e2_log:info(az_bridge_zmq_bound),
    {noreply, State#state{zrouter=Router}}.

setup_amqp_queue(Connection, Bindings) ->
    {Channel, Queue} = create_amqp_queue(Connection),
    subscribe_amqp_queue(Channel, Queue),
    add_amqp_bindings(Bindings, Channel, Queue).

create_amqp_queue(Connection) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    QDecl = #'queue.declare'{auto_delete=true, exclusive=true},
    #'queue.declare_ok'{queue=Queue} = amqp_channel:call(Channel, QDecl),
    {Channel, Queue}.

subscribe_amqp_queue(Channel, Queue) ->
    Sub = #'basic.consume'{queue=Queue, no_ack=true},
    #'basic.consume_ok'{} = amqp_channel:call(Channel, Sub).

add_amqp_bindings([], _Channel, _Queue) -> ok;
add_amqp_bindings([{Exchange, Binding}|Rest], Channel, Queue) ->
    QBind = #'queue.bind'{queue=Queue, exchange=Exchange, routing_key=Binding},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QBind),
    add_amqp_bindings(Rest, Channel, Queue);
add_amqp_bindings([Binding|Rest], Channel, Queue) ->
    add_amqp_bindings([{<<"amq.topic">>, Binding}|Rest], Channel, Queue).

term_zmq_context(#state{zcontext=C}) ->
    e2_log:info(az_bridge_zmq_context_term),
    ok = erlzmq:term(C, ?ZCONTEXT_TERM_TIMEOUT).

close_zmq_router(#state{zrouter=undefined}) -> ok;
close_zmq_router(#state{zrouter=Router}) ->
    e2_log:info(az_bridge_zmq_router_close),
    ok = erlzmq:close(Router).

close_amqp_connection(#state{aconn=undefined}) -> ok;
close_amqp_connection(#state{aconn=C}) ->
    e2_log:info(az_bridge_amqp_connection_close),
    amqp_connection:close(C).
