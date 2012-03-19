-module(xmq_routes_tests).

-include_lib("eunit/include/eunit.hrl").

direct_test() ->
    {ok, R} = xmq_routes:start_link(),

    xmq_routes:add_direct(R, <<"abc">>, abc),
    ?assertEqual([abc], xmq_routes:get_direct(R, <<"abc">>)),

    %% Routes are unique per binding
    xmq_routes:add_direct(R, <<"abc">>, def),
    ?assertEqual([abc, def], sort(xmq_routes:get_direct(R, <<"abc">>))),

    %% Routes must deleted with their binding
    xmq_routes:delete_direct(R, <<"abc">>, def),
    ?assertEqual([abc], xmq_routes:get_direct(R, <<"abc">>)),
    xmq_routes:delete_direct(R, <<"abc">>, abc),
    ?assertEqual([], xmq_routes:get_direct(R, <<"abc">>)).

topic_test() ->
    {ok, R} = xmq_routes:start_link(),

    xmq_routes:add_topic_bindings(R, [<<"a.#">>], b1),
    xmq_routes:add_topic_bindings(R, [<<"a.b">>], b2),
    xmq_routes:add_topic_bindings(R, [<<"a.b.#">>], b3),

    ?assertEqual([b1], sort(xmq_routes:get_topic(R, <<"a">>))),
    ?assertEqual([b1, b2, b3], sort(xmq_routes:get_topic(R, <<"a.b">>))),
    ?assertEqual([b1, b3], sort(xmq_routes:get_topic(R, <<"a.b.c">>))),
    ?assertEqual([b1, b3], sort(xmq_routes:get_topic(R, <<"a.b.c.d">>))),
    ?assertEqual([], sort(xmq_routes:get_topic(R, <<"b">>))),
    ?assertEqual([], sort(xmq_routes:get_topic(R, <<"b.a">>))).

expires_test() ->
    {ok, R} = xmq_routes:start_link(),

    %% Set ttl on routes to 1 second
    xmq_routes:add_direct(R, <<"abc">>, abc, {ttl, 1}),
    xmq_routes:add_topic_bindings(R, [<<"abc.#">>], abc, {ttl, 1}),
    ?assertEqual([abc], xmq_routes:get_direct(R, <<"abc">>)),
    ?assertEqual([abc], xmq_routes:get_topic(R, <<"abc">>)),

    %% Wait 1 second
    timer:sleep(1000),
    ?assertEqual([], xmq_routes:get_direct(R, <<"abc">>)),
    ?assertEqual([], xmq_routes:get_topic(R, <<"abc">>)).

sort(L) -> lists:sort(L).
