-module(amqp_selective_consumer).
-behaviour(gen_statem).

%% API
-export([start_link/0, register_default_consumer/2, consume/2, cancel/2]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, state_wait_for_consume/3, state_consuming/3, state_default/3]).

-record(state_data, {
    channel_pid = undefined,  % The PID of the AMQP channel
    consumers = #{},          % Map of consumer_tag -> Pid
    default_consumer = none,  % Default consumer process
    monitors = #{},           % Monitor refs for consumer processes
    mc_counter_1 = 0          % Some counter (example from original code)
}).

%%% API Functions

%% Starts the selective consumer process.
start_link() ->
    gen_statem:start_link(?MODULE, [], []).

%% Registers a default consumer.
register_default_consumer(ChannelPid, Pid) ->
    gen_statem:cast(self(), {register_default_consumer, ChannelPid, Pid}).

%% Tells the consumer to start consuming messages.
consume(ChannelPid, Queue) ->
    gen_statem:cast(self(), {start_consume, ChannelPid, Queue}).

%% Cancels consumption for a specific consumer.
cancel(ChannelPid, ConsumerTag) ->
    gen_statem:cast(self(), {cancel_consume, ChannelPid, ConsumerTag}).

%%% gen_statem Callback Mode
callback_mode() ->
    state_functions.

%%% gen_statem Init Function
init([]) ->
    {ok, state_wait_for_consume, #state_data{}}.

%%% State: Waiting for Consumption Start
%% This state is where we register a default consumer and wait for consumption to start.

state_wait_for_consume(cast, {register_default_consumer, ChannelPid, Pid}, StateData) ->
    %% Register the default consumer and save its PID
    Ref = monitor(process, Pid),
    io:format("Registered default consumer: ~p~n", [Pid]),
    {next_state, state_wait_for_consume, StateData#state_data{
        channel_pid = ChannelPid,
        default_consumer = Pid,
        monitors = maps:put(Pid, Ref, StateData#state_data.monitors)
    }};
state_wait_for_consume(cast, {start_consume, ChannelPid, Queue}, StateData) ->
    %% Issue the basic consume command and move to the consuming state
    io:format("Starting to consume from queue: ~p~n", [Queue]),
    amqp_gen_consumer:send_basic_consume(ChannelPid, Queue),
    {next_state, state_consuming, StateData};
state_wait_for_consume(_EventType, _Msg, Data) ->
    {keep_state, Data}.

%%% State: Consuming
%% This state is when the consumer is actively consuming messages.

state_consuming(cast, {_Pid, {basic_consume_ok, Tag}}, StateData) ->
    %% Register the consumer tag and its PID
    UpdatedConsumers = maps:put(Tag, self(), StateData#state_data.consumers),
    io:format("Registered consumer tag: ~p~n", [Tag]),
    {next_state, state_default, StateData#state_data{consumers = UpdatedConsumers}};

%% Handle message delivery from RabbitMQ
state_consuming(info, {basic_deliver, Tag, Delivery}, StateData) ->
    %% Forward the message to the appropriate consumer
    case maps:get(Tag, StateData#state_data.consumers, undefined) of
        undefined ->
            io:format("Unknown consumer tag ~p~n", [Tag]),
            {keep_state, StateData};
        Pid ->
            Pid ! {basic_deliver, Delivery},
            {keep_state, StateData}
    end;

%% Handle consumer cancellation
state_consuming(cast, {cancel_consume, _ChannelPid, Tag}, StateData) ->
    %% Remove the consumer associated with the tag
    UpdatedConsumers = maps:remove(Tag, StateData#state_data.consumers),
    io:format("Cancelled consumer with tag ~p~n", [Tag]),
    {next_state, state_default, StateData#state_data{consumers = UpdatedConsumers}};

state_consuming(info, {'DOWN', Ref, process, Pid, _Reason}, StateData) ->
    %% Handle consumer process termination
    io:format("Consumer process ~p terminated~n", [Pid]),
    UpdatedMonitors = maps:filter(fun(_, MRef) -> MRef =/= Ref end, StateData#state_data.monitors),
    UpdatedConsumers = maps:filter(fun(_Tag, ConsumerPid) -> ConsumerPid =/= Pid end, StateData#state_data.consumers),
    {next_state, state_default, StateData#state_data{monitors = UpdatedMonitors, consumers = UpdatedConsumers}};

%%% State: Default State after Consuming Starts
%% This state is where the consumer continues its operations after successfully starting to consume.

state_default(cast, {register_default_consumer, ChannelPid, Pid}, StateData) ->
    %% Update the default consumer
    Ref = monitor(process, Pid),
    io:format("Updated default consumer: ~p~n", [Pid]),
    {keep_state, StateData#state_data{
        channel_pid = ChannelPid,
        default_consumer = Pid,
        monitors = maps:put(Pid, Ref, StateData#state_data.monitors)
    }};
state_default(info, {basic_deliver, Tag, Delivery}, StateData) ->
    %% Handle message delivery
    case maps:get(Tag, StateData#state_data.consumers, undefined) of
        undefined ->
            io:format("Unknown consumer tag ~p~n", [Tag]),
            {keep_state, StateData};
        Pid ->
            Pid ! {basic_deliver, Delivery},
            {keep_state, StateData}
    end;
state_default(info, {'DOWN', Ref, process, Pid, _Reason}, StateData) ->
    %% Handle termination of consumer process
    io:format("Consumer process ~p terminated~n", [Pid]),
    UpdatedMonitors = maps:filter(fun(_, MRef) -> MRef =/= Ref end, StateData#state_data.monitors),
    UpdatedConsumers = maps:filter(fun(_Tag, ConsumerPid) -> ConsumerPid =/= Pid end, StateData#state_data.consumers),
    {keep_state, StateData#state_data{monitors = UpdatedMonitors, consumers = UpdatedConsumers}};
state_default(_EventType, _Msg, Data) ->
    {keep_state, Data}.
