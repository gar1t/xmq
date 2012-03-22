-module(xmq_az_bridge).

-include_lib("amqp_client/include/amqp_client.hrl").

-behavior(e2_service).

-export([start_link/1, dump_info/1]).

-export([init/1, handle_msg/3, terminate/2]).

-record(state, {direct_binding,
                direct_queues,
                topic_bindings,
                aconn,
                achan,
                zcontext,
                zrouter,
                routes,
                binding_ttl,
                cleanup}).

-define(DEFAULT_AMQP_HOST, "localhost").
-define(DEFAULT_AMQP_CREDS, {<<"guest">>, <<"guest">>}).
-define(DEFAULT_ROUTER_ENDPOINT, "tcp://*:5555").
-define(DEFAULT_DIRECT_BINDING, {<<"amq.direct">>, <<"">>}).
-define(DEFAULT_TOPIC_BINDINGS, {<<"amq.topic">>, [<<"#">>]}).
-define(DEFAULT_BINDING_TTL, 60).

-define(ZCONTEXT_TERM_TIMEOUT, 5000).
-define(CLEANUP_INTERVAL, 60000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Options) ->
    {ServiceOpts, BridgeOpts} =
        e2_service_impl:split_options(?MODULE, Options),
    e2_service:start_link(?MODULE, [BridgeOpts], ServiceOpts).

dump_info(Bridge) ->
    e2_service:call(Bridge, dump_info).

%%%===================================================================
%%% Service callbacks
%%%===================================================================

init([Options]) ->
    start_amqp_connect(Options),
    {ZmqContext, ZmqRouter} = init_zmq_router(Options),
    Routes = new_routes(),
    {ok, #state{direct_binding=direct_binding(Options),
                direct_queues=sets:new(),
                topic_bindings=topic_bindings(Options),
                zcontext=ZmqContext,
                zrouter=ZmqRouter,
                routes=Routes,
                binding_ttl=binding_ttl(Options),
                cleanup=start_cleanup()}}.

handle_msg(dump_info, _From, State) ->
    {reply, info(State), State};
handle_msg({amqp_connect, Connection}, _From, State) ->
    handle_amqp_connect(Connection, State);
handle_msg({#'basic.deliver'{exchange=Exchange, routing_key=Key}, Msg},
           _From, State) ->
    handle_amqp_to_zmq(Exchange, Key, Msg, State);
handle_msg({zmq, Router, Client, [rcvmore]}, _From, State) ->
    handle_zmq_msg(Client, erlzmq_util:recv_parts(Router), State);
handle_msg('$cleanup', _From, State) ->
    handle_cleanup(State);
handle_msg({'EXIT', _Pid, normal}, _From, State) ->
    {noreply, State};
handle_msg({'basic.consume_ok', _CTag}, _From, State) ->
    {noreply, State};
handle_msg({'basic.cancel', _CTag, true}, _From, State) ->
    {noreply, State};
handle_msg(Msg, _From, State) ->
    e2_log:info({unhandled_az_bridge_msg, Msg}),
    {noreply, State}.

terminate(_Reason, State) ->
    close_zmq_router(State),
    term_zmq_context(State),
    close_amqp_connection(State).

%%%===================================================================
%%% Internal: init
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

start_cleanup() ->
    e2_task_impl:start_repeat(?CLEANUP_INTERVAL, '$cleanup').

amqp_params(Options) ->
    Host = proplists:get_value(amqp_host, Options, ?DEFAULT_AMQP_HOST),
    {User, Pwd} =
        proplists:get_value(amqp_creds, Options, ?DEFAULT_AMQP_CREDS),
    #amqp_params_network{host=Host, username=User, password=Pwd}.

direct_binding(Options) ->
    proplists:get_value(direct_binding, Options, ?DEFAULT_DIRECT_BINDING).

topic_bindings(Options) ->
    proplists:get_value(topic_bindings, Options, ?DEFAULT_TOPIC_BINDINGS).

router_endpoint(Options) ->
    proplists:get_value(router_endpoint, Options, ?DEFAULT_ROUTER_ENDPOINT).

binding_ttl(Options) ->
    proplists:get_value(binding_ttl, Options, ?DEFAULT_BINDING_TTL).

%%%===================================================================
%%% Internal: debugging / info
%%%===================================================================

info(#state{direct_binding={DirectExch, BridgeQueue},
            direct_queues=Queues,
            topic_bindings={TopicExch, BridgeTopics},
            routes=Routes}) ->
    [{direct_exchange, DirectExch},
     {topic_exchange, TopicExch},
     {bridge_queue, BridgeQueue},
     {bridge_topics, BridgeTopics},
     {routes, xmq_routes:dump_routes(Routes)},
     {direct_queues, sets:to_list(Queues)}].

%%%===================================================================
%%% Internal: AMQP setup
%%%===================================================================

handle_amqp_connect(Connection, State0) ->
    Channel = open_amqp_channel(Connection),
    State = State0#state{aconn=Connection, achan=Channel},
    Queue = create_bridge_amqp_queue(State),
    setup_amqp_topic_bindings(Queue, State),
    {noreply, State}.

open_amqp_channel(Connection) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Channel.

create_bridge_amqp_queue(#state{direct_binding={Exchange, QueueName}}=State) ->
    Queue = declare_amqp_queue(QueueName, State),
    amqp_subscribe(Queue, State),
    amqp_bind(Queue, Exchange, Queue, State),
    Queue.

setup_amqp_topic_bindings(Queue, #state{topic_bindings=Topics}=State) ->
    {Exch, Bindings} = Topics,
    lists:foreach(
      fun(Binding) -> amqp_bind(Queue, Exch, Binding, State) end, Bindings).

declare_amqp_queue(Name, #state{achan=Channel}) ->
    QDecl = #'queue.declare'{queue=Name, auto_delete=true, exclusive=true},
    #'queue.declare_ok'{queue=Queue} = amqp_channel:call(Channel, QDecl),
    Queue.

amqp_subscribe(Queue, #state{achan=Channel}) ->
    Sub = #'basic.consume'{queue=Queue, no_ack=true},
    #'basic.consume_ok'{} = amqp_channel:call(Channel, Sub).

amqp_bind(Queue, Exchange, Binding, #state{achan=Channel}) ->
io:format("##### amqp_bind: ~p~n", [{Queue, Exchange, Binding}]),
    QBind = #'queue.bind'{queue=Queue, exchange=Exchange, routing_key=Binding},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QBind).

%%%===================================================================
%%% Internal: AMQP -> ZMQ message
%%%===================================================================

handle_amqp_to_zmq(Exch, Key, Msg, #state{direct_binding={Exch, _}}=State) ->
    maybe_send_zmq(direct_routes(Key, State), Key, Msg, State),
    {noreply, State};
handle_amqp_to_zmq(Exch, Key, Msg, #state{topic_bindings={Exch, _}}=State) ->
    maybe_send_zmq(topic_routes(Key, State), Key, Msg, State),
    {noreply, State}.

direct_routes(Key, #state{routes=Routes}) ->
    xmq_routes:get_direct(Routes, Key).

topic_routes(Key, #state{routes=Routes}) ->
    xmq_routes:get_topic(Routes, Key).

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
%%% Internal: ZMQ -> AMQP message / binding setup
%%%===================================================================

handle_zmq_msg(Client, [<<"bind">>, Direct|Topics], State) ->
    {noreply, add_zmq_bindings(Client, Direct, Topics, State)};
handle_zmq_msg(Client, [<<"unbind">>, Direct|Topics], State) ->
    {noreply, delete_zmq_bindings(Client, Direct, Topics, State)};
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

add_zmq_bindings(Client, Direct, Topics, State) ->
    add_zmq_direct(Client, Direct, State),
    add_zmq_topic_bindings(Client, Topics, State),
    maybe_create_client_amqp_queue(
      Direct, client_amqp_queue_exists(Direct, State), State).

add_zmq_direct(Client, Direct, #state{routes=R, binding_ttl=TTL}) ->
    case catch(xmq_routes:add_direct(R, Direct, Client, {ttl, TTL})) of
        ok -> ok;
        {'EXIT', Err} ->
            e2_log:error({az_bridge_add_direct, {Err, Direct}})
    end.

add_zmq_topic_bindings(Client, Topics, #state{routes=R, binding_ttl=TTL}) ->
    case catch(xmq_routes:add_topic_bindings(R, Topics, Client, {ttl, TTL})) of
        ok -> ok;
        {'EXIT', Err} ->
            e2_log:error({az_bridge_add_topic_bindings, {Err, Topics}})
    end.

client_amqp_queue_exists(Queue, #state{direct_queues=Queues}) ->
    sets:is_element(Queue, Queues).

maybe_create_client_amqp_queue(_QueueName, true, State) -> State;
maybe_create_client_amqp_queue(QueueName, false, State) ->
    add_direct_queue(create_client_amqp_queue(QueueName, State), State).

create_client_amqp_queue(QueueName, #state{direct_binding={Exch, _}}=State) ->
    Queue = declare_amqp_queue(QueueName, State),
    amqp_bind(Queue, Exch, Queue, State),
    amqp_subscribe(Queue, State),
    Queue.

add_direct_queue(Queue, #state{direct_queues=Queues}=State) ->
    State#state{direct_queues=sets:add_element(Queue, Queues)}.

delete_zmq_bindings(Client, Direct, Topics, #state{routes=R}=State) ->
    catch(xmq_routes:delete_direct(R, Direct, Client)),
    catch(xmq_routes:delete_topic_bindings(R, Topics, Client)),
    delete_orphaned_direct_queues(State).

decode_amqp_msg(Bin) ->
    handle_decode_amqp_msg_result(catch(binary_to_term(Bin))).

handle_decode_amqp_msg_result(#amqp_msg{}=Msg) -> {ok, Msg};
handle_decode_amqp_msg_result({'EXIT', Err}) -> {error, Err}.

%%%===================================================================
%%% Internal: state cleanup
%%%===================================================================

handle_cleanup(State) ->
    delete_expired_routes(State),
    {noreply, next_cleanup(delete_orphaned_direct_queues(State))}.

delete_expired_routes(#state{routes=Routes}) ->
    xmq_routes:delete_expired(Routes).

direct_queues(#state{direct_queues=Queues}) ->
    sets:to_list(Queues).

delete_orphaned_direct_queues(State) ->
    delete_orphaned_direct_queues(direct_queues(State), State).

delete_orphaned_direct_queues([], State) -> State;
delete_orphaned_direct_queues([Queue|Rest], State) ->
    Direct = direct_routes(Queue, State),
    delete_orphaned_direct_queues(
      Rest, maybe_delete_orphaned_queue(Queue, Direct, State)).

maybe_delete_orphaned_queue(Queue, [], State) ->
    delete_amqp_queue(Queue, State),
    delete_direct_queue(Queue, State);
maybe_delete_orphaned_queue(_Queue, _Routes, State) ->
    State.

delete_amqp_queue(Queue, #state{achan=Channel}) ->
    Delete = #'queue.delete'{queue=Queue},
    #'queue.delete_ok'{} = amqp_channel:call(Channel, Delete).

delete_direct_queue(Queue, #state{direct_queues=Queues}=State) ->
    State#state{direct_queues=sets:del_element(Queue, Queues)}.

next_cleanup(#state{cleanup=Cleanup}=State) ->
    State#state{cleanup=e2_task_impl:next_repeat(Cleanup)}.

%%%===================================================================
%%% Internal: term / cleanup
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
