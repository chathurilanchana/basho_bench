%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_riakclient).

-export([new/1,
         run/4]).
-include("basho_bench.hrl").
-include("riak_kv_causal_service.hrl").
-record(state, { client,
                 socket,
                 max_ts::integer(),
                 client_id::integer(),
                 timer_interval,
                 batch_count,
                 batched_labels,
                 server_ip,
                 server_port

    }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client

    TimerInterval=basho_bench_config:get(timer_interval, 1000),
    Concurrent=basho_bench_config:get(concurrent),

    ConfigId=basho_bench_config:get(client_id,1),
    ClientId=(ConfigId-1)*Concurrent+Id,
    Server_Ip=basho_bench_config:get(server_ip,'127.0.0.1'),
    Server_Port=basho_bench_config:get(server_port,50000),
    Cookie  = basho_bench_config:get(riakclient_cookie, 'riak'),
    MyNode  = basho_bench_config:get(riakclient_mynode, [basho_bench, longnames]),
    {ok, Socket} = gen_tcp:connect(Server_Ip, Server_Port, [binary, {active,true}]),
  %% Try to spin up net_kernel
  case net_kernel:start(MyNode) of
    {ok, _} ->
      ?INFO("Net kernel started as ~p\n", [node()]);
    {error, {already_started, _}} ->
      ok;
    {error, Reason} ->
      ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
  end,


    ?INFO("Using target node  for worker ~p\n", [ Id]),

            {ok, #state { client = client,
                          client_id = ClientId,
                          timer_interval = TimerInterval,
                          max_ts =  0,
                          batch_count = 0,
                          batched_labels = [],
                          socket = Socket,
                          server_ip = Server_Ip,
                          server_port = Server_Port}}.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case (State#state.client):get(bucket, Key,State#state.max_ts,2) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State=#state{server_ip = Server_Ip,server_port = Server_Port,socket = Socket}) ->
    Key= KeyGen(),
    Timestamp=get_timestamp(),
    UpdatedMaxTS=max(State#state.max_ts,Timestamp),
    Node_id=State#state.client_id,

     %Label=list_to_binary(string:join([integer_to_list(Timestamp),integer_to_list(Key)],";")),
     Label=string:join([integer_to_list(Timestamp),integer_to_list(Key)],":"),
     Batched_Labels=[Label|State#state.batched_labels],

     Batch_Count=State#state.batch_count+1,
     Timer_Interval=State#state.timer_interval,

   if
        Batch_Count>=Timer_Interval ->
          %Reversed_List=lists:reverse(State#state.batched_labels),

          Label_Str=generate_label_str(State#state.batched_labels,$;),
          Payload=generate_string(Node_id,UpdatedMaxTS,Label_Str),
          Payload_Length=length(Payload),
          Padded_Payload_Header=get_padded_payload_length(Payload_Length),
          Str_With_Header=string:join([Padded_Payload_Header,Payload],"|"),
          io:format("label str is ~p ~n",[Str_With_Header]),
          %Json=mochijson2:encode({struct,[{<<"partition">>,Node_id},{<<"stablets">>,UpdatedMaxTS},{<<"labels">>, {array, Reversed_List}}]}),
          %send_json_to_server(Json,Server_Ip,Server_Port),

          Socket1=send_json_to_server(Str_With_Header,Server_Ip,Server_Port,Socket),

          State1=State#state{batched_labels =[],batch_count = 0,socket = Socket1 };
            %do json conversion here
        true ->
          State1=State#state{batched_labels = Batched_Labels,batch_count = Batch_Count}
   end,
    {ok,State1#state{max_ts  = UpdatedMaxTS}}.


%% ====================================================================
%% Internal functions
%% ====================================================================

get_padded_payload_length(Payload_Length)->
  PayloadHeaderStr=integer_to_list(Payload_Length),
  string:right(PayloadHeaderStr, 8, $0).


generate_label_str(Items, Sep) ->
  lists:flatten(generate_label_str1(Items, Sep, [])).

generate_label_str1([Head | []], _Sep, Acc) ->
  [Head | Acc];
generate_label_str1([Head | Tail], Sep, Acc) ->
  generate_label_str1(Tail, Sep, [Sep, Head | Acc]).

generate_string(NodeId,UpdatedMaxTS,LabelStr)->
  integer_to_list(NodeId) ++ "|" ++ integer_to_list(UpdatedMaxTS) ++"|" ++ LabelStr.

send_json_to_server(Json,Server_Ip,Server_Port,Socket)->
  %{ok, Socket} = gen_tcp:connect(Server_Ip, Server_Port, [binary, {active,true}]),  %reuse same channel
  case gen_tcp:send(Socket, Json) of
      ok->Socket;
      _-> {ok, Socket1} = gen_tcp:connect(Server_Ip, Server_Port, [binary, {active,true}]),gen_tcp:send(Socket1, Json),Socket1
  end.

get_timestamp()->
    {MegaSecs, Secs, MicroSecs}=os:timestamp(),
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.