-module(xmq_app).

-behavior(e2_application).

-export([init/0]).

init() ->
    {ok, az_bridges()}.

az_bridges() ->
    handle_az_bridges_env(application:get_env(az_bridges)).

handle_az_bridges_env(undefined) -> [];
handle_az_bridges_env({ok, BridgesOpts}) ->
    [{xmq_az_bridge, start_link, [Opts]} || Opts <- BridgesOpts].
