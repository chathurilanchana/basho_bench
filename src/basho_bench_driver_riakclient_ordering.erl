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
-module(basho_bench_driver_riakclient_ordering).

-export([new/1,
         run/4,run/2]).

-include("basho_bench.hrl").
-include("riak_kv_causal_service.hrl").
-record(state, { client,
                 target_node,
                 timer_interval,
                 batch_count,
                 bucket,
                 batched_labels,
                 req_id,
                 maxTS,
                 client_id,
                 replies }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(ordering_app) of
        non_existing ->
            ?FAIL_MSG("~s requires ordering app module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Nodes   = basho_bench_config:get(riakclient_nodes),
    Cookie  = basho_bench_config:get(riakclient_cookie, 'ordering'),
    MyNode  = basho_bench_config:get(riakclient_mynode, [basho_bench, longnames]),
    Sequencers=basho_bench_config:get(sequencer, 'ordering@127.0.0.1'),
    Replies = basho_bench_config:get(riakclient_replies, 2),
    Bucket  = basho_bench_config:get(riakclient_bucket, <<"test">>),
    Concurrent=basho_bench_config:get(concurrent),

    ConfigId=basho_bench_config:get(client_id,1),
    ClientId=(ConfigId-1)*Concurrent+Id,
    io:format("actual client id is ~p ~n",[ClientId]),

    TimerInterval=basho_bench_config:get(timer_interval, 1000),


%% Try to spin up net_kernel
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ?INFO("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
    end,

    %% Initialize cookie for each of the nodes
     [true = erlang:set_cookie(N, Cookie) || N <- Sequencers],

    %% Try to ping each of the nodes
    % ping_each(Nodes),
    ping_each(Sequencers),

    %% Choose the node using our ID as a modulus
    TargetNode = lists:nth((Id rem length(Nodes)+1), Nodes),
    ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    connect_kernal(Sequencers),


            {ok, #state { client = ordering_app,
                          target_node=TargetNode,
                          timer_interval = TimerInterval,
                          bucket = Bucket,
                          maxTS = 0,
                          batch_count = 0,
                          batched_labels = [],
                          client_id = ClientId,
                          replies = Replies }}.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case (State#state.client):get(State#state.bucket, Key, State#state.replies) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, _ValueGen, State) ->
    Key= KeyGen(),
    Timestamp=get_timestamp(),
    UpdatedMaxTS=max(State#state.maxTS,Timestamp),
    Req_Id=mk_reqid(),
    Node_id=self(),
    Label=#label{bkey = Key,req_id = Req_Id,timestamp = UpdatedMaxTS,node_id = Node_id},
    Batched_Labels=[Label|State#state.batched_labels],

    Batch_Count=State#state.batch_count+1,
    Timer_Interval=State#state.timer_interval,

    if
        Batch_Count>=Timer_Interval ->
                 Reversed_List=lists:reverse(State#state.batched_labels),
                 State1=case (State#state.client):forward_to_ordering_service(Reversed_List,State#state.client_id,State#state.maxTS) of
                     ok ->
                         State#state{batched_labels =[],batch_count = 0 };
                     {error, Reason} ->
                         io:format("error occured while batch delivering labels ~n"),
                         {error, Reason, State}
                 end;
        true ->
                State1=State#state{batched_labels = Batched_Labels,batch_count = Batch_Count}
    end,
    {ok,State1#state{maxTS = UpdatedMaxTS}};

run(update, KeyGen, ValueGen, State) ->
    Key= KeyGen(),
    Timestamp=get_timestamp(),
    UpdatedMaxTS=max(State#state.maxTS,Timestamp),
    Req_Id=mk_reqid(),
    Node_id=self(),
    Label=#label{bkey = Key,req_id = Req_Id,timestamp = UpdatedMaxTS,node_id = Node_id},
    case (State#state.client):get(State#state.bucket, Key, State#state.replies) of
        {ok, Robj} ->
            _Robj2 = riak_object:update_value(Robj, ValueGen()),
            case (State#state.client):forward_to_ordering_service(Label,State#state.client_id) of
                ok ->
                    {ok, State#state{maxTS  = UpdatedMaxTS}};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            case (State#state.client):forward_to_ordering_service(Label,State#state.client_id) of
                ok ->
                    {ok, State#state{maxTS  = UpdatedMaxTS}};
                {error, Reason} ->
                    {error, Reason, State}
            end
    end;
run(delete, KeyGen, _ValueGen, State) ->
    case (State#state.client):delete(State#state.bucket, KeyGen(), State#state.replies) of
        ok ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

run(batch,State)->
    io:format("list is ~p ~n",[State#state.batched_labels]),
    Reversed_List=lists:reverse(State#state.batched_labels),
    io:format("reversed list is ~p ~n",[Reversed_List]),
    case (State#state.client):forward_to_ordering_service(Reversed_List,State#state.client_id,State#state.maxTS) of
         ok ->
        {ok, State#state{batched_labels =[] }};
        {error, Reason} -> {error, Reason, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
mk_reqid() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

get_timestamp()->
    {MegaSecs, Secs, MicroSecs}=os:timestamp(),
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

ping_each([]) ->
    ok;
ping_each([Node | Rest]) ->
    case net_adm:ping(Node) of
        pong ->
            ping_each(Rest);
        pang ->
            ?FAIL_MSG("Failed to ping node ~p\n", [Node])
    end.
    
connect_kernal([])->
    ok;

connect_kernal([Node|Rest])->
    net_kernel:connect_node(Node),
    global:sync(),
    connect_kernal(Rest).
