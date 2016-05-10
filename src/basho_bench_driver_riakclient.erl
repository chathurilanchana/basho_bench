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

-record(state, { client,
                 target_node,
                 bucket,
                 replies,
                 max_ts=0,
                 gst=0,
                 put_count=0

    }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riak_client) of
        non_existing ->
            ?FAIL_MSG("~s requires riak_client module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Nodes   = basho_bench_config:get(riakclient_nodes),
    Cookie  = basho_bench_config:get(riakclient_cookie, 'riak'),
    MyNode  = basho_bench_config:get(riakclient_mynode, [basho_bench, longnames]),
    Replies = basho_bench_config:get(riakclient_replies, 2),
    Bucket  = basho_bench_config:get(riakclient_bucket, <<"test">>),
    Cluster_Members= basho_bench_config:get(riakclient_cluster_members),% ping and connect kernal to all members to avoid errors

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
    [true = erlang:set_cookie(N, Cookie) || N <- Cluster_Members],

    %% Try to ping each of the nodes
    ping_each(Cluster_Members),

    %connect_kernal(Cluster_Members),

    %% Choose the node using our ID as a modulus
    TargetNode = lists:nth((Id rem length(Nodes)+1), Nodes),
    ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    case riak:client_connect(TargetNode) of
        {ok, Client} ->
            {ok, #state { client = Client,
                          target_node=TargetNode,
                          put_count = 0,
                          bucket = Bucket,
                          replies = Replies }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed get a riak:client_connect to ~p: ~p\n", [TargetNode, Reason2])
    end.

run(get, KeyGen, _ValueGen, State=#state{gst=GSTC}) ->
    Key = KeyGen(),
    case (State#state.client):get(State#state.bucket, Key,State#state.gst, State#state.replies) of
        {{ok, Val},GST} ->
             %io:format("GST received by vnode is ~p mine is ~p ~n",[GST,GSTC]),
             MaxGST=max(GST,GSTC),
             {_D1,TSBIN} = riak_object:get_value(Val),
             TS=binary_to_term(TSBIN),
             %io:format("ts from obj is ~p ~n",[TS]),
             MaxTS=max(TS,State#state.max_ts) ,
            {ok, State#state{max_ts = MaxTS,gst=MaxGST}};
        {{error, notfound},_} ->
            {ok, State};
        {{error, Reason},_}->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    MaxTS=State#state.max_ts,
    Robj = riak_object:new(State#state.bucket, KeyGen(), ValueGen()), %object will be updated at vnode to reflect correct ts

    case riak_client:put(Robj,MaxTS, State#state.replies,{riak_client,[State#state.target_node,undefined]}) of
        {ok,Timestamp} ->
            UpdatedMaxTS=max(MaxTS,Timestamp),
            Put_Count= State#state.put_count+1,
            %io:format("updated Max TS is ~p myid is ~p ~n",[UpdatedMaxTS,self()]),
            {ok, State#state{max_ts = UpdatedMaxTS,put_count = Put_Count}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    MaxTS=State#state.max_ts,
    case (State#state.client):get(State#state.bucket, Key,MaxTS,State#state.replies) of
        {ok, Robj} ->
            Robj2 = riak_object:update_value(Robj, ValueGen()),
            case (State#state.client):put(Robj2,MaxTS, State#state.replies) of
                {ok,Timestamp}->
                    UpdatedMaxTS=max(MaxTS,Timestamp),
                     %io:format("updated max ts is ~p myid is ~p ~n",[UpdatedMaxTS,self()]),
                    Put_Count= State#state.put_count+1,
                    %lager:info("put count is ~p id is ~p ~n",[Put_Count,self()]),
                    {ok, State#state{max_ts = UpdatedMaxTS,put_count = Put_Count}};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            Robj = riak_object:new(State#state.bucket, Key, ValueGen()),
            case (State#state.client):put(Robj,MaxTS, State#state.replies) of
                {ok,Timestamp} ->
                    UpdatedMaxTS=max(MaxTS,Timestamp),
                    Put_Count= State#state.put_count+1,
                    %lager:info("put count is ~p id is ~p ~n",[Put_Count,self()]),
                    %io:format("updated max ts is ~p myid is ~p ~n",[UpdatedMaxTS,self()]),
                    {ok, State#state{max_ts = UpdatedMaxTS,put_count = Put_Count}};
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
    end;
run(test, _KeyGen, _ValueGen, State) ->
     io:format("calling actual driver ~p ~n",[State]),
    Put_Count=State#state.put_count,
     io:format("TOTAL PUTS BY THIS THREAD IS ~p ~n",[Put_Count]),
     ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

ping_each([]) ->
    ok;
ping_each([Node | Rest]) ->
    case net_adm:ping(Node) of
        pong ->
            ping_each(Rest);
        pang ->
            io:format("Failed to ping node ~p ~n", [Node])
    end.

connect_kernal([])->
    ok;

connect_kernal([Node|Rest])->
    net_kernel:connect_node(Node),
    global:sync(),
    connect_kernal(Rest).
