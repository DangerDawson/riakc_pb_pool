-module(riakc_pb_pool).

-behaviour(gen_server).

%TODO: automatically set a client id if it is not passed e.g. reduct the arity for all the commands 
-export([delete/3, get/3,
	 list_buckets/1, list_keys/2, mapred/3, put/2, put/3, max_riak_clients/1,
     checkout_pid/0, checkin_pid/1,
	 start/0, start/1, make_client_id/1, kick_stuck_requests/0,
	 start_link/0, start_link/1, stats/0, stop/0]).

-export([code_change/3, handle_call/3, handle_cast/2,
	 handle_info/2, init/1, terminate/2]).

-define(SERVER, ?MODULE).

-define(DEFAULT_RIAK_HOST, "0.0.0.0").

-define(DEFAULT_RIAK_PORT, 8087).

-define(DEFAULT_RIAK_CLIENTS, 20).

-define(KICK_STUCK_REQUETS_INTERVAL, 1000).

-record(state,
    { riak_client_queue, 
      riak_host, 
      riak_port, 
      checkout_queue, 
      riak_client_dict, 
      max_riak_clients, 
      riak_client_deaths, 
      client_deaths, 
      adjust_clients,
      youngest_client }).

start() -> start([]).

start(Opts) when is_list(Opts) ->
    gen_server:start({local, ?SERVER}, ?MODULE, Opts, []).

start_link() -> start_link([]).

start_link(Opts) when is_list(Opts) ->
    io:format("RIAKKKKKKKKKKKKKKK start_link: Q:~p~n", [ Opts ] ),
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

stop() ->
    case whereis(?SERVER) of
      undefined -> ok;
      _ -> gen_server:cast(?SERVER, stop)
    end.

put(ClientID, Object) ->
    put(ClientID, Object, []).

put(ClientID, Object, Options) ->
    request( ClientID, put, [ Object, Options ]).

get(ClientID, Bucket, Key) ->
    request( ClientID, get, [ Bucket, Key ]).

delete(ClientID, Bucket, Key) ->
    request( ClientID, delete, [ Bucket, Key ]).

mapred(ClientID, Inputs, Query) ->
    request( ClientID, mapred, [ Inputs, Query ]).

list_buckets(ClientID) ->
    request( ClientID, list_buckets, []).

list_keys(ClientID, Bucket) ->
    request( ClientID, list_keys, [ Bucket ]).

make_client_id(Term) ->
    erlang:phash2(Term).

request(ClientID, Request, Args ) when is_integer(ClientID) ->
    case checkout_pid() of
        {ok, RiakPid} ->
            Reply = do_request( RiakPid, ClientID, Request, Args ),
            checkin_pid(RiakPid),
            Reply;
        {error, Error} -> 
            {error, Error}
    end.


stats() -> gen_server:call(?SERVER, stats).

checkout_pid() ->
    gen_server:call(?SERVER, checkout_pid, infinity ).

checkin_pid(Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {checkin_pid, Pid}, infinity ).

kick_stuck_requests() ->
    gen_server:call(?SERVER, kick_stuck_requests, infinity ).

max_riak_clients(RiakClients) ->
    gen_server:call(?SERVER, {max_riak_clients, RiakClients}, infinity ).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    Start = get_opt(riak_clients, Opts),
    Host = get_opt(riak_host, Opts),
    Port = get_opt(riak_port, Opts),
    Interval = get_opt(kick_stuck_requests_interval, Opts),
    io:format("RIAKKKKKKKKKKKKKKK init: Q:~p~n", [ Opts ] ),
    State = #state{
              max_riak_clients = Start,
              riak_client_queue = queue:new(),
              riak_client_dict = dict:new(),
		      riak_port = Port, 
              riak_host = Host,
		      checkout_queue = queue:new(),
              youngest_client = epoch(),
              riak_client_deaths = 0,
              client_deaths = 0,
              adjust_clients = false
          },
    io:format("RIAKKKKKKKKKKKKKKK init: Q:~p~n", [ Opts ] ),
    State2 = build_riak_pool(State),
    io:format("RIAKKKKKKKKKKKKKKK State2: Q:~p~n", [ State2 ] ),
    timer:apply_interval(Interval, riakc_pb_pool, kick_stuck_requests, []),
    {ok, State2}.

handle_call(stats, _From, State ) ->
    ReadableState = [ 
        { max_riak_clients, State#state.max_riak_clients },
        { riak_clients_alive, dict:size(State#state.riak_client_dict) },
        { riak_clients_idle, queue:len(State#state.riak_client_queue) },
        { riak_client_deaths, State#state.riak_client_deaths },
        { client_deaths, State#state.client_deaths },
        { youngest_client, epoch() - State#state.youngest_client },
        { queued_work, queue:len(State#state.checkout_queue) },
        { riak_port, State#state.riak_port },
        { riak_host, State#state.riak_host }
    ],    
    {reply, {ok, ReadableState}, State};
handle_call(checkout_pid, From, State) ->
    case checkout_riak_client(From, State) of
      {RiakPid, State2} -> 
          {reply, {ok, RiakPid}, State2};
      undefined ->
	      NewQueue = queue:in(From, State#state.checkout_queue),
	      State2 = State#state{checkout_queue = NewQueue},
	      {noreply, State2}
    end;
handle_call({checkin_pid, RiakPid}, _From, State) ->
    State2 = checkin_riak_client( RiakPid, State ),
    State3 = serve_queued(State2),
    {reply, ok, State3};
handle_call({max_riak_clients, RiakClients}, _From, State) ->
    State2 = State#state{max_riak_clients = RiakClients},
    State3 = build_riak_pool(State2), 
    {reply, ok, State3};
handle_call(kick_stuck_requests, _From, State) ->
    State2 = build_riak_pool(State), 
    State3 = serve_queued(State2),
    {reply, ok, State3}.

handle_cast(stop, State) -> {stop, normal, State}.

% Client Exits, or Also called if there is a exception in the Riak Client
handle_info({'DOWN', ExitMonitorRef, process, _Pid, _ExitReason}, State = #state{riak_client_dict = Dict, riak_client_queue = _Queue}) ->
    %falcon_logger:error( riakc_pb_pool, "Client: ~100p Exited", [Pid]),
    case lists:keyfind( ExitMonitorRef, 2, dict:to_list( Dict ) ) of
        { RiakPid, _MonitorRef } -> 
            Restarts = State#state.client_deaths + 1,
            State2 = checkin_riak_client( RiakPid, State ),
            State3 = serve_queued( State2 ),
            {noreply, State3#state{client_deaths = Restarts}};
        false ->
            {noreply, State}
    end;
handle_info({'EXIT', _Pid, normal}, State) ->
    %falcon_logger:error( riakc_pb_pool, "Exit message (normal)", []),
    {noreply, State};
% Called if the RiakClient exits e.g. Riak goes away
handle_info({'EXIT', ExitPid, _Reason}, State = #state{riak_client_queue = Queue, riak_client_dict = Dict} ) ->
    %falcon_logger:error( riakc_pb_pool, "Riak Client: ~p Exited with message : ~100p,  Restarting!!", [ExitPid, Reason]),
    ValidRiakClients = queue:filter(fun(RiakPid) -> RiakPid =/= ExitPid end, Queue),
    Dict2 = dict:erase( ExitPid, Dict ),  
    Restarts = State#state.riak_client_deaths + 1,
    {noreply, State#state{riak_client_deaths = Restarts, riak_client_queue = ValidRiakClients, riak_client_dict = Dict2 }};
handle_info(_Msg, State) ->
    %falcon_logger:error( riakc_pb_pool, "Unexpected message: ~10000p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_opt(Opt, Opts) ->
    Default = default_opt(Opt),
    proplists:get_value(Opt, Opts, Default).

default_opt(Opt) ->
    case Opt of
      riak_clients -> ?DEFAULT_RIAK_CLIENTS;
      riak_host -> ?DEFAULT_RIAK_HOST;
      riak_port -> ?DEFAULT_RIAK_PORT;
      kick_stuck_requests_interval -> ?KICK_STUCK_REQUETS_INTERVAL;
      _ -> undefined
    end.

do_request( RiakPid, ClientID, Request, Args ) ->
    {ok, OrigClientId} = riakc_pb_socket:get_client_id(RiakPid),
    ok = riakc_pb_socket:set_client_id(RiakPid, <<ClientID:32>>),
    Response = apply( riakc_pb_socket, Request, [ RiakPid | Args ] ),
    ok = riakc_pb_socket:set_client_id(RiakPid, OrigClientId),
    Response.

build_riak_pool(State = #state{ max_riak_clients  = MaxClients, riak_client_dict = Dict } ) ->
    build_riak_pool( State, MaxClients - dict:size(Dict) ).

build_riak_pool( State, 0 ) ->
    State;
build_riak_pool( State, Count ) ->
    Queue = State#state.riak_client_queue,
    Dict = State#state.riak_client_dict,
    Host = State#state.riak_host,
    Port = State#state.riak_port,
    {Queue2, Dict2} = case create_riak_client( Host, Port ) of
        {ok, Pid} ->
            { queue:in( Pid, Queue ), dict:store(Pid, undefined, Dict) };
        {error,econnrefused} ->
            { Queue, Dict }
    end,
    State2 = State#state{riak_client_queue = Queue2, riak_client_dict = Dict2 },
    build_riak_pool(State2, Count - 1).

create_riak_client(Host, Port) ->
    riakc_pb_socket:start_link(Host, Port).

checkout_riak_client( { ClientPid , _Ref } = _From, State = #state{ riak_client_queue = Queue, riak_client_dict = Dict }) ->
    case queue:out(Queue) of
        {{value, RiakPid}, Queue2} ->
            MonitorRef = erlang:monitor(process, ClientPid),
            Dict2 = dict:store( RiakPid, MonitorRef, Dict ),
            {RiakPid, State#state{riak_client_queue = Queue2, riak_client_dict = Dict2}};
        {empty, _Queue2} ->
            undefined 
    end.

checkin_riak_client(RiakPid, State = #state{ riak_client_queue = Queue, riak_client_dict = Dict }) ->
    case is_process_alive(RiakPid) of
        true ->
            demonitor_client(RiakPid, Dict),
            Queue2 = queue:in(RiakPid, Queue),
            Dict2 = dict:store( RiakPid, undefined, Dict ),
            State#state{riak_client_queue = Queue2, riak_client_dict = Dict2};
        false ->
            Dict2 = dict:erase( RiakPid, Dict ),
            build_riak_pool( State#state{ riak_client_dict = Dict2 } )
    end.

demonitor_client(RiakPid, Dict) ->
    case dict:fetch(RiakPid, Dict) of
        undefined ->
            ok;
        MonitorRef ->
            erlang:demonitor( MonitorRef )
    end.

serve_queued(State) ->
    {Next, State2} = next_client(State),
    case Next of
      {value, {_ClientPid, _Ref} = Client} ->
	      case checkout_riak_client(Client, State2) of
	          {RiakPid, State3} ->
		          gen_server:reply(Client, {ok, RiakPid}), State3;
	          undefined -> 
                  State
	      end;
      empty -> 
          State2
    end.

next_client(#state{checkout_queue = Queue} = State) ->
    case queue:out(Queue) of
        {{value, {Pid, _Ref}} = NextClient, NewQueue} ->
            NewState = State#state{checkout_queue = NewQueue},
            case is_process_alive(Pid) of
                true -> {NextClient, NewState};
                false -> next_client(NewState)
            end;
        {empty, _NewQueue} -> {empty, State}
    end.

epoch() ->
  calendar:datetime_to_gregorian_seconds(erlang:localtime()).
