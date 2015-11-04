%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. 十一月 2015 4:50 PM
%%%-------------------------------------------------------------------
-module(redis_udon_new).
-author("zy").

-behaviour(redis_protocol).

%% API
-export([init/1, handle_redis/3, handle_info/3, terminate/1]).

-record(redis_udon_state, {
    op_num, op_num_r, op_num_w,
    op_timeout,
    enable_forward}).

-compile([{parse_transform, lager_transform}]).

-define(DEFAULT_BUCKET, <<"default">>).

init(_Args) ->
    [
        {nrw, {OpNum, OpNumR, OpNumW}},
        {op_timeout, OpTimeout},
        _,
        {enable_forward, EnableForward}
    ] = udon_api_config:udon_config(),
    {ok, #redis_udon_state{
        op_num = OpNum, op_num_r = OpNumR, op_num_w = OpNumW,
        op_timeout = OpTimeout,
        enable_forward = EnableForward
    }}.

handle_redis(Connection, Action, State = #redis_udon_state{
    op_num = OpNum, op_num_r = OpNumR, op_num_w = OpNumW,
    op_timeout = OpTimeout,
    enable_forward = EnableForward}) ->
    case udon_request(Action) of
        {ok, Bucket, Key, Operate} ->
            case udon_op:op(OpNum, OpNumW, Operate, {Bucket, Key}, OpTimeout, EnableForward) of
                {ok, Value} ->
                    case udon_response(op, Operate, Value) of
                        {ok, Value2} ->
                            ok = redis_protocol:answer(Connection, Value2);
                        {error, Error} ->
                            response_error(Connection, Error)
                    end;
                {forward_disable, TargetNode} ->
                    ErrorDesc = jiffy:encode({[{<<"forward">>, TargetNode}]}),
                    ok = redis_protocol:answer(Connection, {error, ErrorDesc});
                {timeout} ->
                    response_error(Connection, timeout)
            end;
        {reply, Reply} ->
            ok = redis_protocol:answer(Connection, Reply);
        {error, Error} ->
            response_error(Connection, Error)
    end,
    {ok, State}.

handle_info(_Connection, Info, State) ->
    lager:error("unknown info: ~p", [Info]),
    {stop, State}.

terminate(_State) ->
    ok.

response_error(Connection, Error) ->
    ErrorDesc = atom_to_binary(Error, utf8),
    ok = redis_protocol:answer(Connection, {error, ErrorDesc}).

udon_request(Command) when length(Command) > 1 ->
    lager:debug("Redis request ~p", [Command]),
    [MethodBin, BucketKeyBin | Rest] = Command,
    {ok, Bucket, Key} = get_bucket_key(BucketKeyBin),
    {ok, Method} = get_method(MethodBin),
    case Method of
        "set" when length(Rest) =:= 1 ->
            [Item] = Rest,
            {ok, Bucket, Key, {transaction, {Bucket, Key}, [[<<"SET">>, BucketKeyBin, Item]]}};
        "get" when length(Rest) =:= 0 ->
            {ok, Bucket, Key, {transaction_with_value, {Bucket, Key}, [[<<"GET">>, BucketKeyBin]]}};
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
        "stat_appkey_online" when length(Rest) =:= 3 ->
            [Appkey, Uid, TTL] = Rest,
            [DayKey, HourKey, MinKey, _SecKey] = get_time_keys(),
            Commands = lists:foldl(fun(TimeKey, Cmd) ->
                ActiveKey = <<"stat,active_", Appkey/binary, "_", TimeKey/binary>>,
                OnlineKey = <<"stat,online_", Appkey/binary, "_", TimeKey/binary>>,
                [["SADD", ActiveKey, Uid] | [["EXPIRE", ActiveKey, TTL] |
                    [["SADD", OnlineKey, Uid] | [["EXPIRE", OnlineKey, TTL] | Cmd]]]]
                                   end, [], [DayKey, HourKey, MinKey]),
            {ok, Bucket, Key, {transaction, {Bucket, Key}, Commands}};
        "stat_appkey_offline" when length(Rest) =:= 3 ->
            [Appkey, Uid, TTL] = Rest,
            [DayKey, HourKey, MinKey, _SecKey] = get_time_keys(),
            Commands = lists:foldl(fun(TimeKey, Cmd) ->
                ActiveKey = <<"stat,active_", Appkey/binary, "_", TimeKey/binary>>,
                OnlineKey = <<"stat,online_", Appkey/binary, "_", TimeKey/binary>>,
                [["SADD", ActiveKey, Uid] | [["EXPIRE", ActiveKey, TTL] |
                    [["SREM", OnlineKey, Uid] | [["EXPIRE", OnlineKey, TTL] | Cmd]]]]
                                   end, [], [DayKey, HourKey, MinKey]),
            {ok, Bucket, Key, {transaction, {Bucket, Key}, Commands}};
        _ ->
            {error, unsupported_command}
    end;
udon_request(Command) when length(Command) =:= 1 ->
    [MethodBin] = Command,
    {ok, Method} = get_method(MethodBin),
    case Method of
        "info" ->
            {_Claimant, RingReady, _Down, _MarkedDown, _Changes} =
                riak_core_status:ring_status(),
            Reply = case RingReady of
                        undefined ->
                            <<"loading:1\r\n">>;
                        _ ->
                            <<"loading:0\r\n">>
                    end,
            {reply, Reply};
        _ ->
            {error, invalid_command}
    end;
udon_request(_Command) ->
    {error, invalid_command}.

udon_response(op, Op = {sadd, {_Bucket, _Key}, _Item}, Value) ->
    case lists:all(fun(Ret) -> Ret == ok end, Value) of
        true -> {ok, 1};
        _ -> operate_error(op, Op, Value)
    end;
udon_response(op, Op = {srem, {_Bucket, _Key}, _Item}, Value) ->
    case lists:all(fun(Ret) -> Ret == ok end, Value) of
        true -> {ok, 1};
        _ -> operate_error(op, Op, Value)
    end;
udon_response(op, Op = {del, _Bucket, _Key}, Value) ->
    case lists:all(fun(Ret) -> Ret == ok end, Value) of
        true -> {ok, 1};
        _ -> operate_error(op, Op, Value)
    end;
udon_response(op, Op = {smembers, _Bucket, _Key}, Value) ->
    case lists:all(fun({Ret, _}) -> Ret == ok end, Value) of
        true ->
            DupData = lists:map(fun({ok, D}) -> D end, Value),
            Data = lists_union(DupData),
            {ok, Data};
        _ -> operate_error(op, Op, Value)
    end;
udon_response(op, Op = {transaction, {_Bucket, _Key}, _CommandList}, Value) ->
    case lists:all(fun(Ret) -> Ret == ok end, Value) of
        true -> {ok, 1};
        _ -> operate_error(op, Op, Value)
    end;
udon_response(op, Op = {command, {_Bucket, _Key}, _Command}, Value) ->
    case lists:all(fun({Ret, _}) -> Ret == ok end, Value) of
        true ->
            [{ok, Data} | _ ] = Value,
            {ok, Data};
        _ -> operate_error(op, Op, Value)
    end;
udon_response(op, Op = {transaction_with_value, {_Bucket, _Key}, _CommandList}, Value) ->
    case lists:all(fun({Ret, _}) -> Ret == ok end, Value) of
        true ->
            [{ok, Data} | _ ] = Value,
            {ok, Data};
        _ -> operate_error(op, Op, Value)
    end;
udon_response(Type, Operate, Error) ->
    operate_error(Type, Operate, Error).

get_method(MethodBin) ->
    MethodStr = binary_to_list(MethodBin),
    {ok, string:to_lower(MethodStr)}.

get_bucket_key(BucketKeyBin) ->
    case string:tokens(binary_to_list(BucketKeyBin), ",") of
        List when length(List) =:= 2 ->
            [BucketStr, KeyStr] = List,
            {ok, list_to_binary(BucketStr), list_to_binary(KeyStr)};
        _ ->
            {ok, ?DEFAULT_BUCKET, BucketKeyBin}
    end.

lists_union(Lists) ->
    lists_union(Lists, gb_sets:new()).

lists_union([], Set) ->
    gb_sets:to_list(Set);
lists_union([List | Rest], Set) ->
    S = gb_sets:from_list(List),
    Set2 = gb_sets:union([S, Set]),
    lists_union(Rest, Set2).

operate_error(Type, Operate, Error) ->
    lager:error("~p:~p failed: ~p", [Type, Operate, Error]),
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