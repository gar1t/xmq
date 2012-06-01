-module(xmq_az_client).

-behavior(e2_service).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/3, start_link/4, start_link/5, send/4]).

-export([init/1, handle_msg/3, terminate/2]).

-record(state, {socket, bindings, msg_handler, bindings_heart, secret}).

-define(BINDINGS_HEART_INTERVAL, 15000).

-define(DEFAULT_SECRET, <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>).

%%%===================================================================
%%% Start / init
%%%===================================================================

start_link(BridgeEndpoint, Queue, Topics) ->
    start_link(BridgeEndpoint, Queue, Topics, self(), ?DEFAULT_SECRET).

start_link(BridgeEndpoint, Queue, Topics, MsgHandler) ->
    start_link(BridgeEndpoint, Queue, Topics, MsgHandler, ?DEFAULT_SECRET).

start_link(BridgeEndpoint, Queue, Topics, MsgHandler, Secret) ->
    validate_msg_handler(MsgHandler),
    validate_secret(Secret),
    validate_bindings([Queue|Topics]),
    e2_service:start_link(
      ?MODULE, [BridgeEndpoint, Queue, Topics, MsgHandler, Secret]).

validate_msg_handler(Pid) when is_pid(Pid) -> ok;
validate_msg_handler({M, F, A}) when is_atom(M), is_atom(F), is_list(A) -> ok;
validate_msg_handler(Handler) when is_function(Handler) ->
    validate_msg_handler_arity(erlang:fun_info(Handler, arity));
validate_msg_handler(_) -> exit(bad_msg_handler).

validate_msg_handler_arity({arity, 1}) -> ok;
validate_msg_handler_arity({airyt, _}) -> exit(bad_msg_handler_arity).

validate_secret(Bin) when is_binary(Bin), size(Bin) == 16 -> ok;
validate_secret(_) -> exit(bad_secret).

validate_bindings([]) -> ok;
validate_bindings([Binding|Rest]) when is_binary(Binding) ->
    validate_bindings(Rest);
validate_bindings(_) -> exit(bad_bindings).

init([BridgeEndpoint, Queue, Topics, MsgHandler, Secret]) ->
    Context = xmq_zmq_context:get_context(),
    {ok, Socket} = erlzmq:socket(Context, [dealer, {active, true}]),
    ok = erlzmq:connect(Socket, BridgeEndpoint),
    maybe_monitor_handler(MsgHandler),
    {ok, #state{socket=Socket,
                bindings=[Queue|Topics],
                msg_handler=MsgHandler,
                bindings_heart=start_bindings_heart(),
		secret=Secret}}.

maybe_monitor_handler(Pid) when is_pid(Pid) ->
    erlang:monitor(process, Pid);
maybe_monitor_handler(_) -> ok.

start_bindings_heart() ->
    e2_task_impl:start_repeat(0, ?BINDINGS_HEART_INTERVAL, '$bindings_heart').

%%%===================================================================
%%% API
%%%===================================================================

send(Client, Exchange, Key, Payload) when is_binary(Payload) ->
    send(Client, Exchange, Key, #amqp_msg{payload=Payload});
send(Client, Exchange, Key, #amqp_msg{}=Msg)
  when is_binary(Exchange), is_binary(Key) ->
    e2_service:call(Client, {send, Exchange, Key, Msg}).

%%%===================================================================
%%% Service dispatch
%%%===================================================================

handle_msg({send, Exchange, Key, #amqp_msg{}=Msg}, _From, State) ->
    handle_send(Exchange, Key, Msg, State);
handle_msg('$bindings_heart', _From, State) ->
    add_bindings(State),
    {noreply, next_bindings_heart(State)};
handle_msg({zmq, Socket, Part1, [rcvmore]}, _From, State) ->
    handle_zmq_msg([Part1|erlzmq_util:recv_parts(Socket)], State);
handle_msg({'DOWN', _Ref, process, _Pid, _Info}, _From, State) ->
    {stop, normal, State};
handle_msg(Msg, _From, State) ->
    e2_log:info({unhandled_az_client_msg, Msg}),
    {noreply, State}.

%%%===================================================================
%%% Terminate
%%%===================================================================

terminate(_Reason, #state{socket=Socket}=State) ->
    delete_bindings(State),
    erlzmq:close(Socket).

%%%===================================================================
%%% Binding management
%%%===================================================================

add_bindings(#state{bindings=Bindings, socket=Socket, secret=Secret}) ->
    send_parts(Socket, [<<"bind">>|Bindings], Secret).

delete_bindings(#state{bindings=Bindings, socket=Socket, secret=Secret}) ->
    send_parts(Socket, [<<"unbind">>|Bindings], Secret).

next_bindings_heart(#state{bindings_heart=Heart}=State) ->
    State#state{bindings_heart=e2_task_impl:next_repeat(Heart)}.

%%%===================================================================
%%% Incomging message
%%%===================================================================

handle_zmq_msg([<<"aes-cbc-128">>, EncKey|Msg], State) ->
    handle_encrypted_zmq_msg({EncKey, Msg}, State);
handle_zmq_msg(_Msg, State) ->
    e2_log:error(invalid_zmq_msg_header),
    {noreply, State}.

handle_encrypted_zmq_msg({EncKey, EncParts}, State) ->
    handle_decrypt_zmq_key(decrypt_zmq_key(EncKey, State), EncParts, State).

decrypt_zmq_key(EncKey, #state{secret=Key}) ->
    case xmq_crypto:unpad(xmq_crypto:aes_cbc_128_decrypt(EncKey, Key)) of
	Key -> match;
	_Mismatch -> nomatch
    end.

handle_decrypt_zmq_key(match, EncParts, State) ->
    decrypt_zmq_parts(EncParts, [], State);
handle_decrypt_zmq_key(nomatch, _EncParts, State) ->
    e2_log:error(zmq_msg_key_mismatch),
    {noreply, State}.

decrypt_zmq_parts([], DecrAcc, State) ->
    handle_decrypted_zmq_msg(lists:reverse(DecrAcc), State);
decrypt_zmq_parts([EncPart|Rest], DecrAcc, State) ->
    handle_decrypt_zmq_part(
      decrypt_zmq_part(EncPart, State), Rest, DecrAcc, State).

decrypt_zmq_part(EncPart, #state{secret=Key}) ->
    try xmq_crypto:aes_cbc_128_decrypt(EncPart, Key) of
	Decr -> {ok, xmq_crypto:unpad(Decr)}
    catch
	error:{badarg, _} -> error
    end.

handle_decrypt_zmq_part({ok, DecrPart}, Rest, DecrAcc, State) ->
    decrypt_zmq_parts(Rest, [DecrPart|DecrAcc], State);
handle_decrypt_zmq_part(error, _Rest, _DecrAcc, State) ->
    e2_log:error(zmq_msg_decrypt_error),
    {noreply, State}.

handle_decrypted_zmq_msg([<<"amqp_msg">>, Key, MsgBin], State) ->
    handle_amqp_msg(Key, MsgBin, State);
handle_decrypted_zmq_msg(Msg, State) ->
    e2_log:error({unhandled_zmq_msg, Msg}),
    {noreply, State}.

handle_amqp_msg(Key, MsgBin, #state{msg_handler=Handler}=State) ->
    dispatch_message(Handler, {Key, binary_to_term(MsgBin)}),
    {noreply, State}.

dispatch_message(Pid, Msg) when is_pid(Pid) ->
    erlang:send(Pid, Msg);
dispatch_message(Fun, Msg) when is_function(Fun) ->
    Fun(Msg);
dispatch_message({M, F, A}, Msg) ->
    erlang:apply(M, F, A ++ [Msg]).

%%%===================================================================
%%% Outgoing message
%%%===================================================================

handle_send(Exchange, Key, Msg, #state{socket=Socket, secret=Secret}=State) ->
    Parts = [<<"send">>, Exchange, Key, term_to_binary(Msg)],
    send_parts(Socket, Parts, Secret),
    {reply, ok, State}.

%%%===================================================================
%%% Send / encryption
%%%===================================================================

send_parts(Socket, Parts, Secret) ->
    Encrypted = encrypt_parts([Secret|Parts], Secret),
    send_parts(Socket, [<<"aes-cbc-128">>|Encrypted]).

encrypt_parts(Parts, Secret) ->
    [xmq_crypto:aes_cbc_128_encrypt(xmq_crypto:pad(Part, 16), Secret)
     || Part <- Parts].

send_parts(Socket, [Last]) ->
    ok = erlzmq:send(Socket, Last);
send_parts(Socket, [Part|More]) ->
    ok = erlzmq:send(Socket, Part, [sndmore]),
    send_parts(Socket, More).

