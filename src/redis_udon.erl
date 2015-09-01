-module(redis_udon).
-behaviour(redis_protocol).

-export([init/1, handle_redis/3, handle_info/3, terminate/1, exchange/3]).

-record(redis_udon_state, {exchange_pid}).
-record(exchange_state, {operate = undefined, timer = undefined, request_id, op_num, op_num_r, op_num_w}).

-compile([{parse_transform, lager_transform}]).

init(Args) ->
    [{nrw, {OpNum, OpNumR, OpNumW}}] = udon_api_config:udon_config(),
    Pid = spawn_link(?MODULE, exchange, [
        self(), Args, #exchange_state{op_num = OpNum, op_num_r = OpNumR, op_num_w = OpNumW}
    ]),
    {ok, #redis_udon_state{exchange_pid = Pid}}.

handle_redis(_Connection, Action, State = #redis_udon_state{exchange_pid = ExchangePid}) ->
    ExchangePid ! {redis_request, Action},
    {ok, State}.

handle_info(Connection, {response, Value}, State) ->
    ok = redis_protocol:answer(Connection, Value),
    {continue, State};
handle_info(Connection, {error, {forward, TargetNode}}, State) ->
    ErrorDesc = jiffy:encode({[{<<"forward">>, TargetNode}]}),
    ok = redis_protocol:answer(Connection, {error, ErrorDesc}),
    {continue, State};
handle_info(Connection, {error, Error}, State) ->
    ErrorDesc = atom_to_binary(Error, utf8),
    ok = redis_protocol:answer(Connection, {error, ErrorDesc}),
    {continue, State};
handle_info(_Connection, Info, State) ->
    lager:error("unknown info: ~p", [Info]),
    {stop, State}.

terminate(#redis_udon_state{exchange_pid = ExchangePid}) ->
    ExchangePid ! close.

exchange(Client, Args, State = #exchange_state{operate = undefined, timer = undefined, op_num = OpNum, op_num_w = OpNumW}) ->
    receive
        {redis_request, Command} ->
            case udon_request(Command) of
                {ok, Bucket, Key, Operate} ->
                    {ok, ReqId} = udon_op_fsm:op(OpNum, OpNumW, Operate, {Bucket, Key}),
                    TimerRef = erlang:start_timer(5000, self(), timeout),
                    exchange(Client, Args, State#exchange_state{operate = Operate, timer = TimerRef, request_id = ReqId});
                {error, Error} ->
                    Client ! {error, Error},
                    exchange(Client, Args, State)
            end;
        {timeout, _TimerRef, _} ->
            exchange(Client, Args, State);
        {_Id, _Value} ->
            exchange(Client, Args, State);
        {forward_disable, _Id, _Node} ->
            exchange(Client, Args, State);
        close ->
            close;
        Unknown ->
            lager:error("Exchange receive unknown message: ~p", [Unknown])
    end;
exchange(Client, Args, State = #exchange_state{operate = Operate, timer = TimerRef, request_id = RequestId}) ->
    receive
        {redis_request, _} ->
            Client ! {error, waiting_response},
            exchange(Client, Args, State);
        {timeout, TimerRef, _} ->
            Client ! {error, timeout},
            exchange(Client, Args, State#exchange_state{operate = undefined, timer = undefined});
        {RequestId, Value} ->
            erlang:cancel_timer(TimerRef),
            case udon_response(Operate, Value) of
                {ok, Value2} ->
                    Client ! {response, Value2};
                {error, Error} ->
                    Client ! {error, Error}
            end,
            exchange(Client, Args, State#exchange_state{operate = undefined, timer = undefined});
        {forward_disable, RequestId, TargetNode} ->
            erlang:cancel_timer(TimerRef),
            Client ! {error, {forward, TargetNode}},
            exchange(Client, Args, State#exchange_state{operate = undefined, timer = undefined});
        {timeout, _TimerRef, _} ->
            exchange(Client, Args, State);
        {_Id, _Value} ->
            exchange(Client, Args, State);
        {forward_disable, _Id, _Node} ->
            exchange(Client, Args, State);
        close ->
            erlang:cancel_timer(TimerRef),
            close;
        Unknown ->
            erlang:cancel_timer(TimerRef),
            lager:error("Exchange receive unknown message: ~p", [Unknown])
    end.

udon_request(Command) when length(Command) > 1 ->
    lager:debug("Redis request ~p", [Command]),
    [MethodBin, BucketKeyBin | Rest] = Command,
    case get_bucket_key(BucketKeyBin) of
        {ok, Bucket, Key} ->
            {ok, Method} = get_method(MethodBin),
            case Method of
                "sadd" when length(Rest) =:= 1 ->
                    [Item] = Rest,
                    {ok, Bucket, Key, {sadd, {Bucket, Key}, Item}};
                "srem" when length(Rest) =:= 1 ->
                    [Item] = Rest,
                    {ok, Bucket, Key, {srem, {Bucket, Key}, Item}};
                "del" when length(Rest) =:= 0 ->
                    {ok, Bucket, Key, {del, Bucket, Key}};
                "smembers" when length(Rest) =:= 0 ->
                    {ok, Bucket, Key, {smembers, Bucket, Key}};
                "expire" when length(Rest) =:= 1 ->
                    [TTL] = Rest,
                    {ok, Bucket, Key, {transaction, {Bucket, Key}, [[<<"EXPIRE">>, BucketKeyBin, TTL]]}};
                "sadd_with_ttl" when length(Rest) =:= 2 ->
                    [Item, TTL] = Rest,
                    {ok, Bucket, Key, {transaction, {Bucket, Key}, [
                        [<<"SADD">>, BucketKeyBin, Item],
                        [<<"EXPIRE">>, BucketKeyBin, TTL]
                    ]}};
                "srem_with_ttl" when length(Rest) =:= 2 ->
                    [Item, TTL] = Rest,
                    {ok, Bucket, Key, {transaction, {Bucket, Key}, [
                        [<<"SREM">>, BucketKeyBin, Item],
                        [<<"EXPIRE">>, BucketKeyBin, TTL]
                    ]}};
                "stat_appkey_online" when length(Rest) =:= 2 ->
                    [Uid, TTL] = Rest,
                    [DayKey, HourKey, MinKey, _SecKey] = get_time_keys(),
                    Commands = lists:foldl(fun(TimeKey, Cmd) ->
                        ActiveKey = <<"active_", Key/binary, "_", TimeKey/binary>>,
                        OnlineKey = <<"online_", Key/binary, "_", TimeKey/binary>>,
                        [["SADD", ActiveKey, Uid] | [["EXPIRE", ActiveKey, TTL] |
                            [["SADD", OnlineKey, Uid] | [["EXPIRE", OnlineKey, TTL] | Cmd]]]]
                    end, [], [DayKey, HourKey, MinKey]),
                    {ok, Bucket, Key, {transaction, {Bucket, Key}, Commands}};
                "stat_appkey_offline" when length(Rest) =:= 2 ->
                    [Uid, TTL] = Rest,
                    [DayKey, HourKey, MinKey, _SecKey] = get_time_keys(),
                    Commands = lists:foldl(fun(TimeKey, Cmd) ->
                        ActiveKey = <<"active_", Key/binary, "_", TimeKey/binary>>,
                        OnlineKey = <<"online_", Key/binary, "_", TimeKey/binary>>,
                        [["SADD", ActiveKey, Uid] | [["EXPIRE", ActiveKey, TTL] |
                            [["SREM", OnlineKey, Uid] | [["EXPIRE", OnlineKey, TTL] | Cmd]]]]
                    end, [], [DayKey, HourKey, MinKey]),
                    {ok, Bucket, Key, {transaction, {Bucket, Key}, Commands}};
                _ ->
                    {error, unsupported_command}
            end;
        not_found ->
            {error, invalid_command}
    end;
udon_request(_Command) ->
    {error, invalid_command}.

udon_response(Op = {sadd, {_Bucket, _Key}, _Item}, Value) ->
    case lists:all(fun(Ret) -> Ret == ok end, Value) of
        true -> {ok, 1};
        _ -> operate_error(Op, Value)
    end;
udon_response(Op = {srem, {_Bucket, _Key}, _Item}, Value) ->
    case lists:all(fun(Ret) -> Ret == ok end, Value) of
        true -> {ok, 1};
        _ -> operate_error(Op, Value)
    end;
udon_response(Op = {del, _Bucket, _Key}, Value) ->
    case lists:all(fun(Ret) -> Ret == ok end, Value) of
        true -> {ok, 1};
        _ -> operate_error(Op, Value)
    end;
udon_response(Op = {smembers, _Bucket, _Key}, Value) ->
    case lists:all(fun({Ret, _}) -> Ret == ok end, Value) of
        true ->
            DupData = lists:map(fun({ok, D}) -> D end, Value),
            Data = lists_union(DupData),
            {ok, Data};
        _ -> operate_error(Op, Value)
    end;
udon_response(Op = {transaction, {_Bucket, _Key}, _CommandList}, Value) ->
    case lists:all(fun(Ret) -> Ret == ok end, Value) of
        true -> {ok, 1};
        _ -> operate_error(Op, Value)
    end;
udon_response(Operate, Error) ->
    operate_error(Operate, Error).

get_method(MethodBin) ->
    MethodStr = binary_to_list(MethodBin),
    {ok, string:to_lower(MethodStr)}.

get_bucket_key(BucketKeyBin) ->
    case string:tokens(binary_to_list(BucketKeyBin), ",") of
        List when length(List) =:= 2 ->
            [BucketStr, KeyStr] = List,
            {ok, list_to_binary(BucketStr), list_to_binary(KeyStr)};
        _ ->
            not_found
    end.

lists_union(Lists) ->
    lists_union(Lists, gb_sets:new()).

lists_union([], Set) ->
    gb_sets:to_list(Set);
lists_union([List | Rest], Set) ->
    S = gb_sets:from_list(List),
    Set2 = gb_sets:union([S, Set]),
    lists_union(Rest, Set2).

operate_error(Operate, Error) ->
    lager:error("~p failed: ~p", [Operate, Error]),
    {error, failed}.

%% UTC Time
get_time_keys() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} =
        calendar:universal_time(),
    DayKey = iolist_to_binary(
        io_lib:format(
            "~.4.0w-~.2.0w-~.2.0w",
            [Year, Month, Day])),
    HourKey = iolist_to_binary(
        io_lib:format(
            "~.4.0w-~.2.0w-~.2.0w-~.2.0w",
            [Year, Month, Day, Hour])),
    MinKey = iolist_to_binary(
        io_lib:format(
            "~.4.0w-~.2.0w-~.2.0w-~.2.0w-~.1.0w",
            [Year, Month, Day, Hour, Min div 10])),
    SecKey = iolist_to_binary(
        io_lib:format(
            "~.4.0w-~.2.0w-~.2.0w-~.2.0w-~.2.0w-~.1.0w",
            [Year, Month, Day, Hour, Min, Sec div 10])),

    [DayKey, HourKey, MinKey, SecKey].