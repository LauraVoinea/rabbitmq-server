% -module(amqp_selective_consumer).
% -behaviour(amqp_gen_consumer).

% -export([start_link/0, init/1, register_default_consumer/2, state1/3, state2/3, state3/3, state4/3, state5/3, state7/3, state9/3]).
% -export([add_to_monitor_dict/2, remove_from_monitor_dict/2]).

% % -include("consumer.hrl").

% -record(state_data, {channel_pid = undefined, consumers = #{}, default_consumer = none, monitors = #{}, mc_counter_1 = 0 :: integer()}).
% % -type state_data() :: #state_data{}.

% %%---------------------------------------------------------------------------
% %% API
% %%---------------------------------------------------------------------------

% % -spec start_link() -> {ok, pid()} | {error, any()}.
% start_link() ->
%   amqp_gen_consumer:start_link(?MODULE, []).

% % -spec register_default_consumer(pid(), pid()) -> ok.
% register_default_consumer(ChannelPid, Pid) ->
%   amqp_gen_consumer:send_register_default_consumer(ChannelPid, Pid).

% %%---------------------------------------------------------------------------
% %% amqp_gen_consumer Callbacks
% %%---------------------------------------------------------------------------

% % -spec init(list()) -> {ok, state1, state_data()}.
% init([]) ->
%   {ok, state1, #state_data{}, [{next_event, internal, {basic_consume}}]}.

% %% State 1: Send basic.consume to the channel
% % -spec state1(atom(), {basic_consume}, state_data()) -> {next_state, state2, state_data()}.
% state1(internal, {basic_consume}, #state_data{channel_pid = ChannelPid} = Data) ->
%   io:format("Sending ~p~n", [basic_consume]),
%   amqp_gen_consumer:send_basic_consume(ChannelPid),
%   {next_state, state2, Data};
% state1(_EventType, _Msg, _Data) ->
%   io:format("Unexpected event in state1, ignoring~n"),
%   {keep_state, _Data}.

% %% State 2: Receive basic.consume_ok and register the consumer tag
% % -spec state2(atom(), {pid(), {basic_consume_ok, binary()}}, state_data()) -> {next_state, state3, state_data()}.
% state2(cast, {_Pid, {basic_consume_ok, Tag}}, #state_data{consumers = Consumers} = Data) ->
%   UpdatedConsumers = maps:put(Tag, self(), Consumers),
%   io:format("Registered consumer tag ~p for pid ~p~n", [Tag, self()]),
%   {next_state, state3, Data#state_data{consumers = UpdatedConsumers}};
% state2(_EventType, _Msg, _Data) ->
%   io:format("Unexpected event in state2, ignoring~n"),
%   {keep_state, _Data}.

% %% State 3: Handle server-sent temp or basic_cancel
% % -spec state3(atom(), {pid(), {temp}} | {pid(), {basic_cancel}}, state_data()) -> {next_state, state4 | state11, state_data()}.
% state3(cast, {ChannelPid, {temp}}, #state_data{channel_pid = ChannelPid} = Data) ->
%   {next_state, state4, Data, [{next_event, internal, basic_cancel_choice}]};
% state3(cast, {_, {basic_cancel, Tag}}, #state_data{consumers = Consumers} = Data) ->
%   UpdatedConsumers = maps:remove(Tag, Consumers),
%   io:format("Cancelled consumer tag ~p~n", [Tag]),
%   {next_state, state11, Data#state_data{consumers = UpdatedConsumers}};
% state3(_EventType, _Msg, _Data) ->
%   io:format("Unexpected event in state3, ignoring~n"),
%   {keep_state, _Data}.

% %% State 4: Handle delivery or basic cancel, with a choice to send basic_cancel
% % -spec state4(atom(), {pid(), {basic_deliver}} | {pid(), {default_consumer_deliver}} | basic_cancel_choice, state_data()) -> {next_state, state5 | state7 | state9, state_data()}.
% state4(cast, {_, {basic_deliver, Tag}}, Data) ->
%   io:format("Received basic_deliver for tag ~p~n", [Tag]),
%   case resolve_consumer(Tag, Data) of
%     {consumer, Pid} -> Pid ! {basic_deliver, Tag};
%     {default, Pid} -> Pid ! {basic_deliver, Tag};
%     error -> exit(no_consumer_found)
%   end,
%   {next_state, state5, Data, [{next_event, internal, {bogus1}}]};
% state4(cast, {ChannelPid, {default_consumer_deliver}}, #state_data{channel_pid = ChannelPid} = Data) ->
%   {next_state, state7, Data, [{next_event, internal, {bogus2}}]};
% state4(internal, basic_cancel_choice, #state_data{channel_pid = ChannelPid} = Data) ->
%   io:format("Making a choice~n"),
%   Choice = rand:uniform(2),
%   case Choice of
%     1 -> {keep_state, Data};
%     2 ->
%       io:format("Sending ~p~n", [basic_cancel]),
%       amqp_gen_consumer:send_basic_cancel(ChannelPid),
%       {next_state, state9, Data}
%   end;
% state4(_EventType, _Msg, _Data) ->
%   io:format("Unexpected event in state4, ignoring~n"),
%   {keep_state, _Data}.

% %% State 5: Handle bogus1, stop after sending
% % -spec state5(atom(), {bogus1}, state_data()) -> {stop, normal, state_data()}.
% state5(internal, {bogus1}, #state_data{channel_pid = ChannelPid} = Data) ->
%   io:format("Sending ~p~n", [bogus1]),
%   amqp_gen_consumer:send_bogus1(ChannelPid),
%   {stop, normal, Data};
% state5(_EventType, _Msg, _Data) ->
%   io:format("Unexpected event in state5, ignoring~n"),
%   {keep_state, _Data}.

% %% State 7: Handle bogus2, stop after sending
% % -spec state7(atom(), {bogus2}, state_data()) -> {stop, normal, state_data()}.
% state7(internal, {bogus2}, #state_data{channel_pid = ChannelPid} = Data) ->
%   io:format("Sending ~p~n", [bogus2]),
%   amqp_gen_consumer:send_bogus2(ChannelPid),
%   {stop, normal, Data};
% state7(_EventType, _Msg, _Data) ->
%   io:format("Unexpected event in state7, ignoring~n"),
%   {keep_state, _Data}.

% %% State 9: Handle basic_cancel_ok
% % -spec state9(atom(), {pid(), {basic_cancel_ok}}, state_data()) -> {next_state, state10, state_data()}.
% state9(cast, {ChannelPid, {basic_cancel_ok}}, #state_data{channel_pid = ChannelPid} = Data) ->
%   {next_state, state10, Data};
% state9(_EventType, _Msg, _Data) ->
%   io:format("Unexpected event in state9, ignoring~n"),
%   {keep_state, _Data}.

% %%---------------------------------------------------------------------------
% %% Helper Functions
% %%---------------------------------------------------------------------------

% %% Resolve consumer by tag or use default consumer
% resolve_consumer(Tag, #state_data{consumers = Consumers, default_consumer = DefaultConsumer}) ->
%   case maps:find(Tag, Consumers) of
%     {ok, ConsumerPid} -> {consumer, ConsumerPid};
%     error -> case DefaultConsumer of
%                none -> error;
%                _ -> {default, DefaultConsumer}
%              end
%   end.

% %% Monitoring functions for consumer processes
% add_to_monitor_dict(Pid, Monitors) ->
%   case maps:find(Pid, Monitors) of
%     error -> maps:put(Pid, {1, erlang:monitor(process, Pid)}, Monitors);
%     {ok, {Count, MRef}} -> maps:put(Pid, {Count + 1, MRef}, Monitors)
%   end.

% remove_from_monitor_dict(Pid, Monitors) ->
%   case maps:get(Pid, Monitors) of
%     {1, MRef} -> erlang:demonitor(MRef), maps:remove(Pid, Monitors);
%     {Count, MRef} -> maps:put(Pid, {Count - 1, MRef}, Monitors)
%   end.


-module(amqp_selective_consumer).

-behaviour(amqp_gen_consumer).

%% Exported functions
-export([start_link/0, init/1]).
-export([state1/3, state2/3, state3/3, state4/3, state5/3, state6/3, state8/3, state11/3]).

%% Include necessary headers
-include_lib("amqp_client.hrl").

%% Type definitions
% -define(STATE_DATA, state_data).
-record(state_data, {
    channel_pid :: pid(),
    callback_module :: module()
}).

-type state_data() :: #state_data{}.
-type event_type() :: internal | cast | info.
-type event_content() :: term().
-type next_state_action() :: {next_state, atom(), state_data()} | {stop, normal, state_data()} | {keep_state, state_data()}.

%% Start link function
-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    amqp_gen_consumer:start_link(?MODULE, []).

%% Init function
-spec init(list()) -> {ok, atom(), state_data()}.
init(_Args) ->
    %% Initialize state data without waiting for processes
    ChannelPid = erlang:self(), %% Placeholder: Replace with actual channel PID
    StateData = #state_data{
        channel_pid = ChannelPid,
        callback_module = ?MODULE
    },
    io:format("Consumer initialized~n"),
    {ok, state1, StateData}.

%% State functions

%%% State1: Register default consumer
-spec state1(event_type(), event_content(), state_data()) -> next_state_action().
state1(internal, {register_default_consumer}, Data) ->
    io:format("Consumer State1: Sending register_default_consumer~n"),
    ChannelPid = Data#state_data.channel_pid,
    amqp_gen_consumer:send_register_default_consumer(ChannelPid),
    {next_state, state2, Data}.

%%% State2: Send basic_consume
-spec state2(event_type(), event_content(), state_data()) -> next_state_action().
state2(internal, {basic_consume}, Data) ->
    io:format("Consumer State2: Sending basic_consume~n"),
    ChannelPid = Data#state_data.channel_pid,
    amqp_gen_consumer:send_basic_consume(ChannelPid),
    {next_state, state3, Data}.

%%% State3: Wait for basic_consume_ok
-spec state3(event_type(), event_content(), state_data()) -> next_state_action().
state3(cast, {basic_consume_ok}, Data) ->
    io:format("Consumer State3: Received basic_consume_ok~n"),
    {next_state, state4, Data}.

%%% State4: Decide to process message or cancel
-spec state4(event_type(), event_content(), state_data()) -> next_state_action().
state4(cast, {process_message}, Data) ->
    io:format("Consumer State4: Processing message~n"),
    {next_state, state5, Data};
state4(cast, {basic_cancel}, Data) ->
    io:format("Consumer State4: Received basic_cancel~n"),
    {stop, normal, Data};
state4(internal, basic_cancel_choice, Data) ->
    %% Implement logic from amqp_selective_consumer
    %% For this example, we'll always keep processing
    {keep_state, Data};
state4(_, _, Data) ->
    {keep_state, Data}.

%%% State5: Wait for basic_deliver
-spec state5(event_type(), event_content(), state_data()) -> next_state_action().
state5(cast, {basic_deliver}, Data) ->
    io:format("Consumer State5: Received basic_deliver~n"),
    {next_state, state6, Data}.

%%% State6: Send processing_complete
-spec state6(event_type(), event_content(), state_data()) -> next_state_action().
state6(internal, {processing_complete}, Data) ->
    io:format("Consumer State6: Sending processing_complete~n"),
    ChannelPid = Data#state_data.channel_pid,
    amqp_gen_consumer:send_processing_complete(ChannelPid),
    {next_state, state4, Data};
state6(_, _, Data) ->
    {keep_state, Data}.

%%% State8: Wait for basic_cancel_ok and terminate
-spec state8(event_type(), event_content(), state_data()) -> next_state_action().
state8(cast, {basic_cancel_ok}, Data) ->
    io:format("Consumer State8: Received basic_cancel_ok~n"),
    {stop, normal, Data}.

%%% State11: Wait for basic_cancel_ok and terminate
-spec state11(event_type(), event_content(), state_data()) -> next_state_action().
state11(cast, {basic_cancel_ok}, Data) ->
    io:format("Consumer State11: Received basic_cancel_ok in state11~n"),
    {stop, normal, Data}.
