%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Feb 2016 10:05
%%%-------------------------------------------------------------------
-author("chathuri").
-record(label,{
    bkey::integer(),
    timestamp::integer(),
    node_id::integer()
}).