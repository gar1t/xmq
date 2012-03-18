-module(xmq_routes).

-behavior(e2_service).

-export([start_link/0,
         add_direct/3,
         add_direct/4,
         get_direct/2,
         delete_direct/3,
         add_topic_bindings/3,
         add_topic_bindings/4,
         get_topic/2,
         delete_topic_bindings/3,
         delete_expired/1,
         dump_routes/1]).

-export([init/1, handle_msg/3]).

-record(state, {tab}).

-define(is_expires_timestamp(T), is_integer(Expires) orelse Expires == never).
-define(is_key(K), is_binary(K)).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    e2_service:start_link(?MODULE, []).

add_direct(Routes, Key, Dest) ->
    add_direct(Routes, Key, Dest, never).

add_direct(Routes, Key, Dest, {ttl, TTL}) ->
    add_direct(Routes, Key, Dest, expires_timestamp(TTL));
add_direct(Routes, Key, Dest, Expires)
  when ?is_key(Key), ?is_expires_timestamp(Expires) ->
    e2_service:call(Routes, {add_direct, Key, Dest, Expires}).

get_direct(Routes, Key) ->
    e2_service:call(Routes, {get_direct, Key}).

delete_direct(Routes, Key, Dest) ->
    e2_service:call(Routes, {delete_direct, Key, Dest}).

add_topic_bindings(Routes, Bindings, Dest) ->
    add_topic_bindings(Routes, Bindings, Dest, never).

add_topic_bindings(Routes, Bindings, Dest, {ttl, TTL}) ->
    add_topic_bindings(Routes, Bindings, Dest, expires_timestamp(TTL));
add_topic_bindings(Routes, Bindings, Dest, Expires)
  when ?is_expires_timestamp(Expires) ->
    validate_bindings(Bindings),
    e2_service:call(Routes, {add_topic_bindings, Bindings, Dest, Expires}).

get_topic(Routes, Key) ->
    e2_service:call(Routes, {get_topic, Key}).

delete_topic_bindings(Routes, Bindings, Dest) ->
    e2_service:call(Routes, {delete_topic_bindings, Bindings, Dest}).

delete_expired(Routes) ->
    e2_service:cast(Routes, delete_expired).

dump_routes(Routes) ->
    e2_service:call(Routes, dump_routes).

%%%===================================================================
%%% Service callbacks
%%%===================================================================

init([]) ->
    Tab = init_ets(),
    {ok, #state{tab=Tab}}.

handle_msg({add_direct, Key, Dest, Expires}, _From, State) ->
    {reply, add(direct_object(Key, Dest, Expires), State), State};
handle_msg({get_direct, Key}, _From, State) ->
    {reply, select({{d, Key, '$1'}, '_'}, ['$1'], State), State};
handle_msg({delete_direct, Key, Dest}, _From, State) ->
    {reply, delete({{d, Key, Dest}, '_'}, State), State};
handle_msg({add_topic_bindings, Bindings, Dest, Expires}, _From, State) ->
    {reply, madd(topic_objects(Bindings, Dest, Expires), State), State};
handle_msg({get_topic, Key}, _From, State) ->
    {reply, select_topic(Key, State), State};
handle_msg({delete_topic_bindings, Bindings, Dest}, _From, State) ->
    {reply, mdelete(topic_objects(Bindings, Dest, '_'), State), State};
handle_msg(delete_expired, _From, State) ->
    {reply, delete({'_', '$1'}, [{'<', '$1', timestamp()}], State), State};
handle_msg(dump_routes, _From, State) ->
    {reply, tab_to_list(State), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_ets() ->
    ets:new(undefined, [private, set]).

add(Object, #state{tab=Tab}) ->
    ets:insert(Tab, Object),
    ok.

madd(Objects, State) ->
    lists:foreach(fun(Object) -> add(Object, State) end, Objects).

select(Match, Select, #state{tab=Tab}) ->
    ets:select(Tab, [{Match, [], Select}]).

delete(Select, State) ->
    delete(Select, [], State).

delete(Select, Guard, #state{tab=Tab}) ->
    ets:select_delete(Tab, [{Select, Guard, [true]}]),
    ok.

mdelete(Selects, State) ->
    lists:foreach(fun(Select) -> delete(Select, State) end, Selects).

direct_object(Key, Dest, Expires) ->
    {{d, Key, Dest}, {Expires, []}}.

topic_objects(Bindings, Dest, Expires) ->
    [topic_object(Binding, Dest, Expires) || Binding <- Bindings].

topic_object(Binding, Dest, Expires) ->
    {BaseBinding, IsWildcard} = parse_binding(Binding),
    {{t, BaseBinding, Dest}, {Expires, t_opts(IsWildcard)}}.

parse_binding(Binding) when size(Binding) > 2 ->
    handle_split_binding(split_binary(Binding, size(Binding) - 2));
parse_binding(Binding) -> {Binding, false}.

handle_split_binding({Base, <<".#">>}) -> {Base, true};
handle_split_binding({P1, P2}) -> {<<P1/binary, P2/binary>>, false}.

t_opts(true) -> ['#'];
t_opts(false) -> [].

select_topic(Key, State) ->
    select_topics({full_key, topic_key_parts(Key)}, State, []).

select_topics({_, []}, State, Acc) -> Acc;
select_topics({KeyType, KeyParts}, State, Acc) ->
    Key = join_key_parts(KeyParts),
    handle_select_topics_result(
      select({{t, Key, '$1'}, {'_', '$2'}}, ['$$'], State),
      KeyType, KeyParts, State, Acc).

handle_select_topics_result(Result, KeyType, KeyParts, State, Acc) ->
    select_topics(
      {partial_key, pop_key_part(KeyParts)}, State,
      add_topic_result(Result, KeyType, Acc)).

add_topic_result([], _KeyType, Acc) -> Acc;
add_topic_result([[Dest, _]|Rest], full_key=KeyType, Acc) ->
    add_topic_result(Rest, KeyType, [Dest|Acc]);
add_topic_result([[Dest, ['#']]|Rest], partial_key=KeyType, Acc) ->
    add_topic_result(Rest, KeyType, [Dest|Acc]);
add_topic_result([_|Rest], partial_key=KeyType, Acc) ->
    add_topic_result(Rest, KeyType, Acc).

topic_key_parts(Key) ->
    binary:split(Key, <<".">>, [global]).

join_key_parts(Parts) ->
    <<".", Key/binary>> = iolist_to_binary([[".", Part] || Part <- Parts]),
    Key.

pop_key_part([_]) -> [];
pop_key_part(Parts) ->
    [_|T] = lists:reverse(Parts),
    lists:reverse(T).

tab_to_list(#state{tab=Tab}) ->
    ets:tab2list(Tab).

timestamp() ->
    {M, S, U} = erlang:now(),
    M * 1000000000 + S * 1000 + U div 1000.

expires_timestamp(Seconds) when is_integer(Seconds) ->
    timestamp() + Seconds * 1000.

validate_bindings([]) -> ok;
validate_bindings([Binding|Rest]) when ?is_key(Binding) ->
    validate_bindings(Rest);
validate_bindings(_) ->
    error(badarg).
