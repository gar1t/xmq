-module(xmq_routes_expire_task).

-behavior(e2_task).

-export([start_link/1]).

-export([handle_task/1]).

-define(INTERVAL, 60000).
-define(DELAY, 60000).

start_link(Routes) ->
    e2_task:start_link(
      ?MODULE, Routes, [{repeat, ?INTERVAL}, {delay, ?DELAY}]).

handle_task(Routes) ->
    xmq_routes:delete_expired(Routes),
e2_log:info({routes, xmq_routes:dump_routes(Routes)}),
    {repeat, Routes}.
