%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. 六月 2015 2:33 PM
%%%-------------------------------------------------------------------
-module(udon_api_config).
-author("zy").

%% API
-export([web_config/0, redis_config/0]).

web_config() ->
    {ok, App}  = application:get_application(?MODULE),
    Ip = application:get_env(App, web_ip, "127.0.0.1"),
    Port = application:get_env(App, web_port, 7777),

    PrivDir        = code:priv_dir(App),
    RF             = filename:join([PrivDir, "dispatch.conf"]),
    {ok, Dispatch} = file:consult(RF),

    [
        {ip, Ip},
        {port, Port},
        {dispatch, Dispatch}
    ].

redis_config() ->
    {ok, App}  = application:get_application(?MODULE),
    Port = application:get_env(App, redis_port, 6379),

    [{port, Port}].