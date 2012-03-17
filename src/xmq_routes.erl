-module(xmq_routes).

-behavior(e2_service).

-export([start_link/0, add_direct/3, add_direct/4, get_direct/2,
         delete_expired/1]).

-export([init/1, handle_msg/3]).

-record(state, {tab}).

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
  when is_integer(Expires); Expires == never ->
    e2_service:call(Routes, {add_direct, Key, Dest, Expires}).

get_direct(Routes, Key) ->
    e2_service:call(Routes, {get_direct, Key}).

delete_expired(Routes) ->
    e2_service:cast(Routes, delete_expired).

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
handle_msg(delete_expired, _From, State) ->
    {reply, delete({'_', '$1'}, [{'<', '$1', timestamp()}], State), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_ets() ->
    ets:new(undefined, [private, set]).

add(Object, #state{tab=Tab}) ->
    ets:insert(Tab, Object),
    ok.

select(Match, Select, #state{tab=Tab}) ->
    ets:select(Tab, [{Match, [], Select}]).

delete(Select, Guard, #state{tab=Tab}) ->
    ets:select_delete(Tab, [{Select, Guard, [true]}]).

timestamp() ->
    {M, S, U} = erlang:now(),
    M * 1000000000 + S * 1000 + U div 1000.

expires_timestamp(Seconds) when is_integer(Seconds) ->
    timestamp() + Seconds * 1000.
