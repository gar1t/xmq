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
    {reply, add({{d, Key, Dest}, Expires}, State), State};
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

topic_objects(Bindings, Dest, Expires) ->
    topic_objects(Bindings, Dest, Expires, []).

topic_objects([], _Dest, _Expires, Acc) -> Acc;
topic_objects([Binding|Rest], Dest, Expires, Acc) ->
    topic_objects(
      Rest, Dest, Expires,
      topic_parts(split_binding(Binding), 1, Dest, Expires, Acc)).

split_binding(Binding) ->
    binary:split(Binding, <<".">>, [global]).

topic_parts([], _Num, _Dest, _Expires, Acc) -> Acc;
topic_parts([Part|Rest], Num, Dest, Expires, Acc) -> 
    topic_parts(
      Rest, Num + 1, Dest, Expires,
      [topic_part(Part, Num, Dest, Expires)|Acc]).

topic_part(Part, Num, Dest, Expires) ->
    {{t, Num, Part, Dest}, Expires}.

select_topic(Key, State) ->
    select_topic(split_binding(Key), 1, State, sets:new(), undefined).

select_topic([], _Num, _State, _AccWildcard, Result) ->
    sets:to_list(Result);
select_topic([Part|Rest], Num, State, AccWildcard0, Result0) ->
    {Explicit, Wildcard} = select_topic_part(Num, Part, State),
    AccWildcard = sets:subtract(sets:union(Wildcard, AccWildcard0), Explicit),
    Result = select_topic_result(Explicit, AccWildcard, Result0),
    select_topic(Rest, Num + 1, State, AccWildcard, Result).

select_topic_result(Explicit, Wildcard, undefined) ->
    sets:union(Explicit, Wildcard);
select_topic_result(Explicit, Wildcard, Result0) ->
    sets:intersection(sets:union(Explicit, Wildcard), Result0).

select_topic_part(Num, Part, State) ->
    {sets:from_list(select({{t, Num, Part, '$1'}, '_'}, ['$1'], State)),
     sets:from_list(select({{t, Num, <<"#">>, '$1'}, '_'}, ['$1'], State))}.

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
