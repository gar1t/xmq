-module(xmq).

-export([start/0]).

start() ->
    e2_application:start_with_dependencies(xmq).
