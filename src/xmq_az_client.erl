-module(xmq_az_client).

-behavior(e2_service).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/3, start_link/4, send/4]).

-export([init/1, handle_msg/3, terminate/2]).

-record(state, {socket, bindings, msg_handler, bindings_heart}).

-define(BINDINGS_HEART_INTERVAL, 15000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(BridgeEndpoint, Queue, Topics) ->
    start_link(BridgeEndpoint, Queue, Topics, self()).

start_link(BridgeEndpoint, Queue, Topics, MsgHandler) ->
    validate_msg_handler(MsgHandler),
    validate_bindings([Queue|Topics]),
    e2_service:start_link(
      ?MODULE, [BridgeEndpoint, Queue, Topics, MsgHandler]).

send(Client, Exchange, Key, Payload) when is_binary(Payload) ->
    send(Client, Exchange, Key, #amqp_msg{payload=Payload});
send(Client, Exchange, Key, #amqp_msg{}=Msg)
  when is_binary(Exchange), is_binary(Key) ->
    e2_service:call(Client, {send, Exchange, Key, Msg}).

%%%===================================================================
%%% Service callbacks
%%%===================================================================

init([BridgeEndpoint, Queue, Topics, MsgHandler]) ->
    Context = xmq_zmq_context:get_context(),
    {ok, Socket} = erlzmq:socket(Context, [dealer, {active, true}]),
    ok = erlzmq:connect(Socket, BridgeEndpoint),
    maybe_monitor_handler(MsgHandler),
    {ok, #state{socket=Socket,
                bindings=[Queue|Topics],
                msg_handler=MsgHandler,
                bindings_heart=start_bindings_heart()}}.

handle_msg({send, Exchange, Key, #amqp_msg{}=Msg}, _From, State) ->
    handle_send(Exchange, Key, Msg, State);
handle_msg('$bindings_heart', _From, State) ->
    add_bindings(State),
    {noreply, next_bindings_heart(State)};
handle_msg({zmq, _Socket, <<"amqp_msg">>, [rcvmore]}, _From, State) ->
    handle_amqp_msg(State);
handle_msg({'DOWN', _Ref, process, _Pid, _Info}, _From, State) ->
    {stop, normal, State};
handle_msg(Msg, _From, State) ->
    e2_log:info({unhandled_az_client_msg, Msg}),
    {noreply, State}.

terminate(_Reason, #state{socket=Socket}=State) ->
    delete_bindings(State),
    erlzmq:close(Socket).

%%%===================================================================
%%% Internal functions
%%%===================================================================

validate_msg_handler(Pid) when is_pid(Pid) -> ok;
validate_msg_handler({M, F, A}) when is_atom(M), is_atom(F), is_list(A) -> ok;
validate_msg_handler(Handler) when is_function(Handler) ->
    validate_msg_handler_arity(erlang:fun_info(Handler, arity));
validate_msg_handler(_) -> exit(bad_msg_handler).

validate_msg_handler_arity({arity, 1}) -> ok;
validate_msg_handler_arity({airyt, _}) -> exit(bad_msg_handler_arity).

validate_bindings([]) -> ok;
validate_bindings([Binding|Rest]) when is_binary(Binding) ->
    validate_bindings(Rest);
validate_bindings(_) -> exit(bad_bindings).

maybe_monitor_handler(Pid) when is_pid(Pid) ->
    erlang:monitor(process, Pid);
maybe_monitor_handler(_) -> ok.

add_bindings(#state{bindings=Bindings, socket=Socket}) ->
    send_parts(Socket, [<<"bind">>|Bindings]).

delete_bindings(#state{bindings=Bindings, socket=Socket}) ->
    send_parts(Socket, [<<"unbind">>|Bindings]).

start_bindings_heart() ->
    e2_task_impl:start_repeat(0, ?BINDINGS_HEART_INTERVAL, '$bindings_heart').

next_bindings_heart(#state{bindings_heart=Heart}=State) ->
    State#state{bindings_heart=e2_task_impl:next_repeat(Heart)}.

send_parts(Socket, [Last]) ->
    ok = erlzmq:send(Socket, Last);
send_parts(Socket, [Part|More]) ->
    ok = erlzmq:send(Socket, Part, [sndmore]),
    send_parts(Socket, More).

handle_amqp_msg(#state{socket=Socket, msg_handler=Handler}=State) ->
    [Key, MsgBin] = erlzmq_util:recv_parts(Socket),
    dispatch_message(Handler, {Key, binary_to_term(MsgBin)}),
    {noreply, State}.

dispatch_message(Pid, Msg) when is_pid(Pid) ->
    erlang:send(Pid, Msg);
dispatch_message(Fun, Msg) when is_function(Fun) ->
    Fun(Msg);
dispatch_message({M, F, A}, Msg) ->
    erlang:apply(M, F, A ++ [Msg]).

handle_send(Exchange, Key, Msg, #state{socket=Socket}=State) ->
    ok = erlzmq:send(Socket, <<"send">>, [sndmore]),
    ok = erlzmq:send(Socket, Exchange, [sndmore]),
    ok = erlzmq:send(Socket, Key, [sndmore]),
    ok = erlzmq:send(Socket, term_to_binary(Msg)),
    {reply, ok, State}.
