%% @author David Dawson
%% @hidden
-module(riakc_pb_pool_app).
-behaviour(application).
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    riakc_pb_pool_sup:start_link().

stop(_State) ->
    ok.
