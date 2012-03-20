-module(xmq_az_bridge).

-include_lib("amqp_client/include/amqp_client.hrl").

-behavior(e2_service).

-export([start_link/1, start_link/2, get_routes/1]).

-export([init/1, handle_msg/3, terminate/2]).

-record(state, {bindings, aconn, achan, zcontext, zrouter, routes,
                binding_ttl}).

-define(DEFAULT_AMQP_PARAMS, #amqp_params_network{}).
-define(DEFAULT_ROUTER_ENDPOINT, "tcp://*:5555").
-define(DEFAULT_BINDING_TTL, 60).

-define(ZCONTEXT_TERM_TIMEOUT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Options) ->
    start_link([<<"#">>], Options).

start_link(Bindings, Options) ->
    {ServiceOpts, BridgeOpts} =
        e2_service_impl:split_options(?MODULE, Options),
    e2_service:start_link(?MODULE, [Bindings, BridgeOpts], ServiceOpts).

get_routes(Bridge) ->
    e2_service:call(Bridge, get_routes).

%%%===================================================================
%%% Service callbacks
%%%===================================================================

init([Bindings, Options]) ->
    start_amqp_connect(Options),
    {ZmqContext, ZmqRouter} = init_zmq_router(Options),
    Routes = new_routes(),
    start_routes_expire_task(Routes),
    {ok, #state{bindings=Bindings,
                zcontext=ZmqContext,
                zrouter=ZmqRouter,
                routes=Routes,
                binding_ttl=binding_ttl(Options)}}.

handle_msg(get_routes, _From, #state{routes=Routes}=State) ->
    {reply, xmq_routes:dump_routes(Routes), State};
handle_msg({amqp_connect, Connection}, _From, State) ->
    handle_amqp_connect(Connection, State);
handle_msg({#'basic.deliver'{routing_key=Key}, Msg}, _From, State) ->
    handle_amqp_to_zmq(Key, Msg, State);
handle_msg({zmq, Router, Client, [rcvmore]}, _From, State) ->
    handle_zmq_msg(Client, erlzmq_util:recv_parts(Router), State);
handle_msg({'EXIT', _Pid, normal}, _From, State) ->
    {noreply, State};
handle_msg({'basic.consume_ok', _CTag}, _From, State) ->
    {noreply, State};
handle_msg(Msg, _From, State) ->
    e2_log:info({unhandled_az_bridge_msg, Msg}),
    {noreply, State}.

terminate(_Reason, State) ->
    close_zmq_router(State),
    term_zmq_context(State),
    close_amqp_connection(State).

%%%===================================================================
%%% Internal init
%%%===================================================================

init_zmq_router(Options) ->
    {ok, Context} = erlzmq:context(),
    {ok, Router} = erlzmq:socket(Context, [router, {active, true}]),
    ok = erlzmq:bind(Router, router_endpoint(Options)),
    {Context, Router}.

new_routes() ->
    {ok, Routes} = xmq_routes:start_link(),
    Routes.

start_amqp_connect(Options) ->
    xmq_amqp_connect:start_link(amqp_params(Options)).

start_routes_expire_task(Routes) ->
    xmq_routes_expire_task:start_link(Routes).

amqp_params(Options) ->
    proplists:get_value(amqp_params, Options, ?DEFAULT_AMQP_PARAMS).

router_endpoint(Options) ->
    proplists:get_value(router_endpoint, Options, ?DEFAULT_ROUTER_ENDPOINT).

binding_ttl(Options) ->
    proplists:get_value(binding_ttl, Options, ?DEFAULT_BINDING_TTL).

%%%===================================================================
%%% Internal AMQP setup
%%%===================================================================

handle_amqp_connect(Connection, #state{bindings=Bindings}=State) ->
    {Channel, _Queue} = setup_amqp_queue(Connection, Bindings),
    {noreply, State#state{aconn=Connection, achan=Channel}}.

setup_amqp_queue(Connection, Bindings) ->
    {Channel, Queue} = create_amqp_queue(Connection),
    subscribe_amqp_queue(Channel, Queue),
    add_amqp_bindings(Bindings, Channel, Queue),
    {Channel, Queue}.

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

%%%===================================================================
%%% Internal: AMQP -> ZMQ message
%%%===================================================================

handle_amqp_to_zmq(Key, Msg, #state{routes=Routes}=State) ->
    maybe_send_zmq(xmq_routes:get_topic(Routes, Key), Key, Msg, State),
    {noreply, State}.

maybe_send_zmq([], _Key, _Msg, _State) -> ok;
maybe_send_zmq(Clients, Key, Msg, #state{zrouter=Router}) ->
    send_zmq(Router, Clients, Key, term_to_binary(Msg)).

send_zmq(_Router, [], _Key, _MsgBin) -> ok;
send_zmq(Router, [Client|Rest], Key, MsgBin) ->
    erlzmq:send(Router, Client, [sndmore]),
    erlzmq:send(Router, <<"amqp_msg">>, [sndmore]),
    erlzmq:send(Router, Key, [sndmore]),
    erlzmq:send(Router, MsgBin),
    send_zmq(Router, Rest, Key, MsgBin).

%%%===================================================================
%%% Internal: ZMQ -> AMQP message or binding setup
%%%===================================================================

handle_zmq_msg(Client, [<<"bind">>|Bindings], State) ->
    add_zmq_bindings(Client, Bindings, State),
    {noreply, State};
handle_zmq_msg(Client, [<<"unbind">>|Bindings], State) ->
    delete_zmq_bindings(Client, Bindings, State),
    {noreply, State};
handle_zmq_msg(_Client, [<<"send">>, Exchange, Key, MsgBin], State) ->
    handle_zmq_to_amqp(Exchange, Key, decode_amqp_msg(MsgBin), State);
handle_zmq_msg(_Client, Msg, State) ->
    e2_log:info({unhandled_az_bridge_zmq_msg, Msg}),
    {noreply, State}.

handle_zmq_to_amqp(Exchange, Key, {ok, #amqp_msg{}=Msg}, State) ->
    send_amqp(Exchange, Key, Msg, State),
    {noreply, State};
handle_zmq_to_amqp(_Exchange, _Key, {error, Err}, State) ->
    e2_log:error({bad_amqp_msg, Err}),
    {noreply, State}.

send_amqp(Exchange, Key, Msg, #state{achan=Channel}) ->
    Pub = #'basic.publish'{exchange=Exchange, routing_key=Key},
    ok = amqp_channel:cast(Channel, Pub, Msg).

add_zmq_bindings(Client, Bindings, #state{routes=Routes, binding_ttl=TTL}) ->
    handle_add_topic_bindings_result(
      catch(
        xmq_routes:add_topic_bindings(Routes, Bindings, Client, {ttl, TTL})),
      Bindings).

handle_add_topic_bindings_result({'EXIT', {badarg, _}}, Bindings) ->
    e2_log:error({az_bridge_illegal_bindings, Bindings});
handle_add_topic_bindings_result(ok, _Bindings) -> ok.

delete_zmq_bindings(Client, Bindings, #state{routes=Routes}) ->
    catch(xmq_routes:delete_topic_bindings(Routes, Bindings, Client)).

decode_amqp_msg(Bin) ->
    handle_decode_amqp_msg_result(catch(binary_to_term(Bin))).

handle_decode_amqp_msg_result(#amqp_msg{}=Msg) -> {ok, Msg};
handle_decode_amqp_msg_result({'EXIT', Err}) -> {error, Err}.

%%%===================================================================
%%% Internal term / cleanup
%%%===================================================================

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
