%% @author David Dawson

-module(riakc_pb_pool_sup).
-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Args = [
        {riak_host, "0.0.0.0"},
        {riak_port, "8087"},
        {riak_clients_start, 5},
        {riak_clients_max, 20}
    ],
    RiakcPbPool = {riakc_pb_pool, {riakc_pb_pool, start_link, [Args]},
        permanent, 5000, worker, [riakc_pb_pool]},
    {ok, {{one_for_one, 5, 30}, [RiakcPbPool]}}.
