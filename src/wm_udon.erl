%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, Yunba.io
%%% @doc
%%%
%%% @end
%%% Created : 17. 六月 2015 3:16 PM
%%%-------------------------------------------------------------------
-module(wm_udon).
-author("zy").

-define(INTERNAL_MODULE, udon).

-record(state, {available_method_list, response_data = undefined}).

-compile([{parse_transform, lager_transform}]).

%% API
-export([init/1, allowed_methods/2, resource_exists/2, content_types_provided/2,
    provide_content/2, post_is_create/2, process_post/2]).

-include_lib("webmachine/include/webmachine.hrl").

init(_Config) ->
    AvailableMethod = ?INTERNAL_MODULE:module_info(exports),
    {ok, #state{available_method_list = AvailableMethod}}.

allowed_methods(ReqData, State) ->
    {['GET', 'POST'], ReqData, State}.

resource_exists(ReqData= #wm_reqdata{method = 'GET'}, State = #state{available_method_list = MethodList}) ->
    {ok, Bucket} = get_bucket(ReqData),
    {ok, Key} = get_key(ReqData),
    {ok, Method} = get_method(ReqData),
    lager:debug("GET: method ~p bucket ~p key ~p", [Method, Bucket, Key]),

    case lists:keyfind(Method, 1, MethodList) of
        {Method, ArgsNum} ->
            case wrq:req_qs(ReqData) of
                [] when ArgsNum =:= 2 ->
                    Result = ?INTERNAL_MODULE:Method(Bucket, Key),
                    {true, ReqData, State#state{response_data = Result}};
                ReqQs when ArgsNum =:= 3 ->
                    Result = ?INTERNAL_MODULE:Method(Bucket, Key, ReqQs),
                    {true, ReqData, State#state{response_data = Result}};
                _ ->
                    {false, ReqData, State}
            end;
        false ->
            {false, ReqData, State}
    end;
resource_exists(ReqData= #wm_reqdata{method = 'POST'}, State = #state{available_method_list = MethodList}) ->
    {ok, Bucket} = get_bucket(ReqData),
    {ok, Key} = get_key(ReqData),
    {ok, Method} = get_method(ReqData),
    ReqBody = wrq:req_body(ReqData),
    lager:debug("POST: method ~p bucket ~p key ~p value ~p", [Method, Bucket, Key, ReqBody]),

    case lists:keyfind(Method, 1, MethodList) of
        {Method, ArgsNum} ->
            case wrq:req_qs(ReqData) of
                [] when ArgsNum =:= 2 ->
                    Result = ?INTERNAL_MODULE:Method({Bucket, Key}, ReqBody),
                    {true, ReqData, State#state{response_data = Result}};
                ReqQs when ArgsNum =:= 3 ->
                    Result = ?INTERNAL_MODULE:Method({Bucket, Key}, ReqBody, ReqQs),
                    {true, ReqData, State#state{response_data = Result}};
                _ ->
                    {false, ReqData, State}
            end;
        false ->
            {false, ReqData, State}
    end.

content_types_provided(ReqData, State) ->
    ContentType = webmachine_util:guess_mime(wrq:disp_path(ReqData)),
    {[{ContentType, provide_content}], ReqData,State}.

provide_content(ReqData, State = #state{response_data = undefined}) ->
    lager:debug("RESPONSE: empty"),
    {"", ReqData, State};
provide_content(ReqData, State = #state{response_data = Data}) when is_binary(Data) ->
    lager:debug("RESPONSE: ~p", [Data]),
    {Data, ReqData, State};
provide_content(ReqData, State = #state{response_data = Data}) ->
    lager:error("RESPONSE: ~p isn't binary", [Data]),
    {{error, resp_format_error}, ReqData, State}.

post_is_create(ReqData, State) ->
    {false, ReqData, State}.

process_post(ReqData, State = #state{response_data = undefined}) ->
    lager:debug("RESPONSE: empty"),
    {true, ReqData, State};
process_post(ReqData, State = #state{response_data = Data}) when is_binary(Data) ->
    lager:debug("RESPONSE: ~p", [Data]),
    ReqData2 = wrq:set_resp_body(Data, ReqData),
    {true, ReqData2, State};
process_post(ReqData, State = #state{response_data = Data}) ->
    lager:error("RESPONSE: ~p isn't binary", [Data]),
    {false, ReqData, State}.

get_bucket(ReqData) ->
    case wrq:path_info('bucket', ReqData) of
        undefined ->
            not_found;
        Value when is_list(Value)  ->
            {ok, list_to_binary(Value)}
    end.

get_key(ReqData) ->
    case wrq:path_info('key', ReqData) of
        undefined ->
            not_found;
        Value when is_list(Value)  ->
            {ok, list_to_binary(Value)}
    end.

get_method(ReqData) ->
    case wrq:path_info('method', ReqData) of
        undefined ->
            not_found;
        Value when is_list(Value)  ->
            {ok, list_to_atom(Value)}
    end.