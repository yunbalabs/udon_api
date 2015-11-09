-module(udon_api_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    Result = udon_api_sup:start_link(),

    [{port, Port}] = udon_api_config:redis_config(),
    redis_protocol:start(Port, redis_udon_new),

    Result.

stop(_State) ->
    ok.