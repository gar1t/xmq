-module(xmq_routes_tests).

-include_lib("eunit/include/eunit.hrl").

topic_test() ->
    {ok, R} = xmq_routes:start_link(),

    xmq_routes:add_topic_bindings(R, [<<"a.#">>], b1),
    xmq_routes:add_topic_bindings(R, [<<"a.b">>], b2),
    xmq_routes:add_topic_bindings(R, [<<"a.b.#">>], b3),
    xmq_routes:add_topic_bindings(R, [<<"a.c">>], b4),
    xmq_routes:add_topic_bindings(R, [<<"a.#.c">>], b5),

    ?assertEqual([b1, b2, b3, b4, b5], xmq_routes:get_topic(R, <<"a">>)),
    ?assertEqual([b1, b2, b3, b5], xmq_routes:get_topic(R, <<"a.b">>)),
    ?assertEqual([b1, b3, b5], xmq_routes:get_topic(R, <<"a.b.c">>)),
    ?assertEqual([b1, b3], xmq_routes:get_topic(R, <<"a.b.c.d">>)).

multi_topic_binding_test() ->
    {ok, R} = xmq_routes:start_link(),

    %xmq_routes:add_topic_bindings(R, [<<"a.#">>, <<"b.#">>], b1),
    xmq_routes:add_topic_bindings(R, [<<"a.b">>, <<"b.b">>], b2),

    ?assertEqual([], xmq_routes:get_topic(R, <<"a">>)),
    ok.
