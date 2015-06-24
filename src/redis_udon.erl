-module(redis_udon).
-behaviour(redis_protocol).

-export([init/1, handle_redis/3, handle_info/3, terminate/1, exchange/3]).

-record(redis_udon_state, {exchange_pid}).
-record(exchange_state, {operate = undefined, timer = undefined, request_id}).

-compile([{parse_transform, lager_transform}]).

init(Args) ->
    Pid = spawn_link(?MODULE, exchange, [self(), Args, undefined]),
    {ok, #redis_udon_state{exchange_pid = Pid}}.

handle_redis(_Connection, Action, State = #redis_udon_state{exchange_pid = ExchangePid}) ->
    ExchangePid ! {redis_request, Action},
    {ok, State}.

handle_info(Connection, {response, Value}, State) ->
    ok = redis_protocol:answer(Connection, Value),
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

exchange(Client, Args, undefined) ->
    receive
        {redis_request, Command} ->
            case udon_request(Command) of
                {ok, Bucket, Key, Operate} ->
                    %%{ok, ReqId} = udon_op_fsm:op(3, 3, Operate, {Bucket, Key}),
                    ReqId = "testid",
                    self() ! {ReqId, ok},
                    TimerRef = erlang:start_timer(5000, self(), timeout),
                    exchange(Client, Args, #exchange_state{operate = Operate, timer = TimerRef, request_id = ReqId});
                {error, Error} ->
                    Client ! {error, Error},
                    exchange(Client, Args, undefined)
            end;
        {timeout, _TimerRef, _} ->
            exchange(Client, Args, undefined);
        {_Id, _Value} ->
            exchange(Client, Args, undefined);
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
            exchange(Client, Args, undefined);
        {RequestId, Value} ->
            erlang:cancel_timer(TimerRef),
            case udon_response(Operate, Value) of
                {ok, Value2} ->
                    Client ! {response, Value2};
                {error, Error} ->
                    Client ! {error, Error}
            end,
            exchange(Client, Args, undefined);
        {timeout, _TimerRef, _} ->
            exchange(Client, Args, State);
        {_Id, _Value} ->
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
    {ok, Method} = get_method(MethodBin),
    case get_bucket_key(BucketKeyBin) of
        {ok, Bucket, Key} ->
            {ok, Bucket, Key, {Method}};
        not_found ->
            {error, invalid_command}
    end;
udon_request(_Command) ->
    {error, invalid_command}.

udon_response(Operate, Value) ->
    lager:debug("Redis request ~p udon response ~p", [Operate, Value]),
    Resp = Value,
    {ok, Resp}.

get_method(MethodBin) ->
    MethodStr = binary_to_list(MethodBin),
    {ok, list_to_atom(string:to_lower(MethodStr))}.

get_bucket_key(BucketKeyBin) ->
    case string:tokens(binary_to_list(BucketKeyBin), ",") of
        List when length(List) =:= 2 ->
            [BucketStr, KeyStr] = List,
            {ok, list_to_binary(BucketStr), list_to_binary(KeyStr)};
        _ ->
            not_found
    end.