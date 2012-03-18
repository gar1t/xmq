-module(xmq_routes_tests).

-include_lib("eunit/include/eunit.hrl").

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

sort(L) -> lists:sort(L).
