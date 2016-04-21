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
-module(basho_bench_driver_riakclient_sequencer_optimised).

-export([new/1,
         run/4]).
-include("basho_bench.hrl").
-include("riak_kv_causal_service.hrl").
-record(state, { client,
                 socket,
                 bucket,
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
    Server_Port=basho_bench_config:get(server_port,50201),
    Cookie  = basho_bench_config:get(riakclient_cookie, 'riak'),
    MyNode  = basho_bench_config:get(riakclient_mynode, [basho_bench, longnames]),
    Bucket = basho_bench_config:get(riakclient_bucket, <<"test">>),
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
                          bucket = Bucket,
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
    Robj = {KeyGen(), ValueGen()},
    B_Message = term_to_binary(Robj),
    io:format("message length ~p ~n",[byte_size(B_Message)]),
    case gen_tcp:send(Socket, B_Message) of
      ok->Socket;
        _-> {ok, Socket1} = gen_tcp:connect(Server_Ip, Server_Port, [binary, {active,true}]),gen_tcp:send(Socket1, B_Message),Socket1
    end,
    {ok,State#state{socket = Socket}}.


%% ====================================================================
%% Internal functions
%% ====================================================================



get_timestamp()->
    {MegaSecs, Secs, MicroSecs}=os:timestamp(),
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.