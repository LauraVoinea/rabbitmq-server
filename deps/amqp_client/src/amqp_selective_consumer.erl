% %% This Source Code Form is subject to the terms of the Mozilla Public
% %% License, v. 2.0. If a copy of the MPL was not distributed with this
% %% file, You can obtain one at https://mozilla.org/MPL/2.0/.
% %%
% %% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
% %%

% %% @doc This module is an implementation of the amqp_gen_consumer
% %% behaviour and can be used as part of the Consumer parameter when
% %% opening AMQP channels. This is the default implementation selected
% %% by channel. <br/>
% %% <br/>
% %% The Consumer parameter for this implementation is {{@module}, []@}<br/>
% %% This consumer implementation keeps track of consumer tags and sends
% %% the subscription-relevant messages to the registered consumers, according
% %% to an internal tag dictionary.<br/>
% %% <br/>
% %% Send a #basic.consume{} message to the channel to subscribe a
% %% consumer to a queue and send a #basic.cancel{} message to cancel a
% %% subscription.<br/>
% %% <br/>
% %% The channel will send to the relevant registered consumers the
% %% basic.consume_ok, basic.cancel_ok, basic.cancel and basic.deliver messages
% %% received from the server.<br/>
% %% <br/>
% %% If a consumer is not registered for a given consumer tag, the message
% %% is sent to the default consumer registered with
% %% {@module}:register_default_consumer. If there is no default consumer
% %% registered in this case, an exception occurs and the channel is abruptly
% %% terminated.<br/>
% -module(amqp_selective_consumer).

% -include("amqp_gen_consumer_spec.hrl").

% -behaviour(amqp_gen_consumer).

% -export([register_default_consumer/2]).
% -export([init/1, handle_consume_ok/3, handle_consume/3, handle_cancel_ok/3,
%          handle_cancel/2, handle_server_cancel/2,
%          handle_deliver/3, handle_deliver/4,
%          handle_info/2, handle_call/3, terminate/2]).

% -record(state, {consumers             = #{}, %% Tag -> ConsumerPid
%                 unassigned            = undefined,  %% Pid
%                 monitors              = #{}, %% Pid -> {Count, MRef}
%                 default_consumer      = none}).

% %%---------------------------------------------------------------------------
% %% Interface
% %%---------------------------------------------------------------------------

% %% @spec (ChannelPid, ConsumerPid) -> ok
% %% where
% %%      ChannelPid = pid()
% %%      ConsumerPid = pid()
% %% @doc This function registers a default consumer with the channel. A
% %% default consumer is used when a subscription is made via
% %% amqp_channel:call(ChannelPid, #'basic.consume'{}) (rather than
% %% {@module}:subscribe/3) and hence there is no consumer pid
% %% registered with the consumer tag. In this case, the relevant
% %% deliveries will be sent to the default consumer.
% register_default_consumer(ChannelPid, ConsumerPid) ->
%     amqp_channel:call_consumer(ChannelPid,
%                                {register_default_consumer, ConsumerPid}).

% %%---------------------------------------------------------------------------
% %% amqp_gen_consumer callbacks
% %%---------------------------------------------------------------------------

% %% @private
% init([]) ->
%     {ok, #state{}}.

% %% @private
% handle_consume(#'basic.consume'{consumer_tag = Tag,
%                                 nowait       = NoWait},
%                Pid, State = #state{consumers = Consumers,
%                                    monitors = Monitors}) ->
%     Result = case NoWait of
%                  true when Tag =:= undefined orelse size(Tag) == 0 ->
%                      no_consumer_tag_specified;
%                  _ when is_binary(Tag) andalso size(Tag) >= 0 ->
%                      case resolve_consumer(Tag, State) of
%                          {consumer, _} -> consumer_tag_in_use;
%                          _             -> ok
%                      end;
%                  _ ->
%                      ok
%              end,
%     case {Result, NoWait} of
%         {ok, true} ->
%             {ok, State#state
%                    {consumers = maps:put(Tag, Pid, Consumers),
%                     monitors  = add_to_monitor_dict(Pid, Monitors)}};
%         {ok, false} ->
%             {ok, State#state{unassigned = Pid}};
%         {Err, true} ->
%             {error, Err, State};
%         {_Err, false} ->
%             %% Don't do anything (don't override existing
%             %% consumers), the server will close the channel with an error.
%             {ok, State}
%     end.

% %% @private
% handle_consume_ok(BasicConsumeOk, _BasicConsume,
%                   State = #state{unassigned = Pid,
%                                  consumers  = Consumers,
%                                  monitors   = Monitors})
%   when is_pid(Pid) ->
%     State1 =
%         State#state{
%           consumers  = maps:put(tag(BasicConsumeOk), Pid, Consumers),
%           monitors   = add_to_monitor_dict(Pid, Monitors),
%           unassigned = undefined},
%     deliver(BasicConsumeOk, State1),
%     {ok, State1}.

% %% @private
% %% We sent a basic.cancel.
% handle_cancel(#'basic.cancel'{nowait = true},
%               #state{default_consumer = none}) ->
%     exit(cancel_nowait_requires_default_consumer);

% handle_cancel(Cancel = #'basic.cancel'{nowait = NoWait}, State) ->
%     State1 = case NoWait of
%                  true  -> do_cancel(Cancel, State);
%                  false -> State
%              end,
%     {ok, State1}.

% %% @private
% %% We sent a basic.cancel and now receive the ok.
% handle_cancel_ok(CancelOk, _Cancel, State) ->
%     State1 = do_cancel(CancelOk, State),
%     %% Use old state
%     deliver(CancelOk, State),
%     {ok, State1}.

% %% @private
% %% The server sent a basic.cancel.
% handle_server_cancel(Cancel = #'basic.cancel'{nowait = true}, State) ->
%     State1 = do_cancel(Cancel, State),
%     %% Use old state
%     deliver(Cancel, State),
%     {ok, State1}.

% %% @private
% handle_deliver(Method, Message, State) ->
%     deliver(Method, Message, State),
%     {ok, State}.

% %% @private
% handle_deliver(Method, Message, DeliveryCtx, State) ->
%     deliver(Method, Message, DeliveryCtx, State),
%     {ok, State}.

% %% @private
% handle_info({'DOWN', _MRef, process, Pid, _Info},
%             State = #state{monitors         = Monitors,
%                            consumers        = Consumers,
%                            default_consumer = DConsumer }) ->
%     case maps:find(Pid, Monitors) of
%         {ok, _CountMRef} ->
%             {ok, State#state{monitors = maps:remove(Pid, Monitors),
%                              consumers =
%                                  maps:filter(
%                                    fun (_, Pid1) when Pid1 =:= Pid -> false;
%                                        (_, _)                      -> true
%                                    end, Consumers)}};
%         error ->
%             case Pid of
%                 DConsumer -> {ok, State#state{
%                                     monitors = maps:remove(Pid, Monitors),
%                                     default_consumer = none}};
%                 _         -> {ok, State} %% unnamed consumer went down
%                                          %% before receiving consume_ok
%             end
%     end.

% %% @private
% handle_call({register_default_consumer, Pid}, _From,
%             State = #state{default_consumer = PrevPid,
%                            monitors         = Monitors}) ->
%     Monitors1 = case PrevPid of
%                     none -> Monitors;
%                     _    -> remove_from_monitor_dict(PrevPid, Monitors)
%                 end,
%     {reply, ok,
%      State#state{default_consumer = Pid,
%                  monitors = add_to_monitor_dict(Pid, Monitors1)}}.

% %% @private
% terminate(_Reason, State) ->
%     State.

% %%---------------------------------------------------------------------------
% %% Internal plumbing
% %%---------------------------------------------------------------------------

% deliver_to_consumer_or_die(Method, Msg, State) ->
%     case resolve_consumer(tag(Method), State) of
%         {consumer, Pid} -> Pid ! Msg;
%         {default, Pid}  -> Pid ! Msg;
%         error           -> exit(unexpected_delivery_and_no_default_consumer)
%     end.

% deliver(Method, State) ->
%     deliver(Method, undefined, State).
% deliver(Method, Message, State) ->
%     Combined = if Message =:= undefined -> Method;
%                   true                  -> {Method, Message}
%                end,
%     deliver_to_consumer_or_die(Method, Combined, State).
% deliver(Method, Message, DeliveryCtx, State) ->
%     Combined = if Message =:= undefined -> Method;
%                   true                  -> {Method, Message, DeliveryCtx}
%                end,
%     deliver_to_consumer_or_die(Method, Combined, State).

% do_cancel(Cancel, State = #state{consumers = Consumers,
%                                  monitors  = Monitors}) ->
%     Tag = tag(Cancel),
%     case maps:find(Tag, Consumers) of
%         {ok, Pid} -> State#state{
%                        consumers = maps:remove(Tag, Consumers),
%                        monitors  = remove_from_monitor_dict(Pid, Monitors)};
%         error     -> %% Untracked consumer. Do nothing.
%                      State
%     end.

% resolve_consumer(Tag, #state{consumers = Consumers,
%                              default_consumer = DefaultConsumer}) ->
%     case maps:find(Tag, Consumers) of
%         {ok, ConsumerPid} -> {consumer, ConsumerPid};
%         error             -> case DefaultConsumer of
%                                  none -> error;
%                                  _    -> {default, DefaultConsumer}
%                              end
%     end.

% tag(#'basic.consume'{consumer_tag = Tag})         -> Tag;
% tag(#'basic.consume_ok'{consumer_tag = Tag})      -> Tag;
% tag(#'basic.cancel'{consumer_tag = Tag})          -> Tag;
% tag(#'basic.cancel_ok'{consumer_tag = Tag})       -> Tag;
% tag(#'basic.deliver'{consumer_tag = Tag})         -> Tag.

% add_to_monitor_dict(Pid, Monitors) ->
%     case maps:find(Pid, Monitors) of
%         error               -> maps:put(Pid,
%                                         {1, erlang:monitor(process, Pid)},
%                                         Monitors);
%         {ok, {Count, MRef}} -> maps:put(Pid, {Count + 1, MRef}, Monitors)
%     end.

% remove_from_monitor_dict(Pid, Monitors) ->
%     case maps:get(Pid, Monitors) of
%         {1, MRef}     -> erlang:demonitor(MRef),
%                          maps:remove(Pid, Monitors);
%         {Count, MRef} -> maps:put(Pid, {Count - 1, MRef}, Monitors)
%     end.

% ============
% TODO

-module(amqp_selective_consumer).
-behaviour(amqp_gen_consumer).

-export([start_link/0, init/1, register_default_consumer/2, state1/3, state2/3, state3/3, state4/3, state5/3, state7/3, state9/3]).
-export([add_to_monitor_dict/2, remove_from_monitor_dict/2]).

% -include("consumer.hrl").

-record(state_data, {channel_pid = undefined, consumers = #{}, default_consumer = none, monitors = #{}, mc_counter_1 = 0 :: integer()}).
-type state_data() :: #state_data{}.

%%---------------------------------------------------------------------------
%% API
%%---------------------------------------------------------------------------

% -spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
  gen_consumer:start_link(?MODULE, []).

% -spec register_default_consumer(pid(), pid()) -> ok.
register_default_consumer(ChannelPid, Pid) ->
  gen_consumer:send_register_default_consumer(ChannelPid, Pid).

%%---------------------------------------------------------------------------
%% gen_consumer Callbacks
%%---------------------------------------------------------------------------

% -spec init(list()) -> {ok, state1, state_data()}.
init([]) ->
  {ok, state1, #state_data{}, [{next_event, internal, {basic_consume}}]}.

%% State 1: Send basic.consume to the channel
% -spec state1(atom(), {basic_consume}, state_data()) -> {next_state, state2, state_data()}.
state1(internal, {basic_consume}, #state_data{channel_pid = ChannelPid} = Data) ->
  io:format("Sending ~p~n", [basic_consume]),
  gen_consumer:send_basic_consume(ChannelPid),
  {next_state, state2, Data};
state1(_EventType, _Msg, _Data) ->
  io:format("Unexpected event in state1, ignoring~n"),
  {keep_state, _Data}.

%% State 2: Receive basic.consume_ok and register the consumer tag
% -spec state2(atom(), {pid(), {basic_consume_ok, binary()}}, state_data()) -> {next_state, state3, state_data()}.
state2(cast, {_Pid, {basic_consume_ok, Tag}}, #state_data{consumers = Consumers} = Data) ->
  UpdatedConsumers = maps:put(Tag, self(), Consumers),
  io:format("Registered consumer tag ~p for pid ~p~n", [Tag, self()]),
  {next_state, state3, Data#state_data{consumers = UpdatedConsumers}};
state2(_EventType, _Msg, _Data) ->
  io:format("Unexpected event in state2, ignoring~n"),
  {keep_state, _Data}.

%% State 3: Handle server-sent temp or basic_cancel
% -spec state3(atom(), {pid(), {temp}} | {pid(), {basic_cancel}}, state_data()) -> {next_state, state4 | state11, state_data()}.
state3(cast, {ChannelPid, {temp}}, #state_data{channel_pid = ChannelPid} = Data) ->
  {next_state, state4, Data, [{next_event, internal, basic_cancel_choice}]};
state3(cast, {_, {basic_cancel, Tag}}, #state_data{consumers = Consumers} = Data) ->
  UpdatedConsumers = maps:remove(Tag, Consumers),
  io:format("Cancelled consumer tag ~p~n", [Tag]),
  {next_state, state11, Data#state_data{consumers = UpdatedConsumers}};
state3(_EventType, _Msg, _Data) ->
  io:format("Unexpected event in state3, ignoring~n"),
  {keep_state, _Data}.

%% State 4: Handle delivery or basic cancel, with a choice to send basic_cancel
% -spec state4(atom(), {pid(), {basic_deliver}} | {pid(), {default_consumer_deliver}} | basic_cancel_choice, state_data()) -> {next_state, state5 | state7 | state9, state_data()}.
state4(cast, {_, {basic_deliver, Tag}}, Data) ->
  io:format("Received basic_deliver for tag ~p~n", [Tag]),
  case resolve_consumer(Tag, Data) of
    {consumer, Pid} -> Pid ! {basic_deliver, Tag};
    {default, Pid} -> Pid ! {basic_deliver, Tag};
    error -> exit(no_consumer_found)
  end,
  {next_state, state5, Data, [{next_event, internal, {bogus1}}]};
state4(cast, {ChannelPid, {default_consumer_deliver}}, #state_data{channel_pid = ChannelPid} = Data) ->
  {next_state, state7, Data, [{next_event, internal, {bogus2}}]};
state4(internal, basic_cancel_choice, #state_data{channel_pid = ChannelPid} = Data) ->
  io:format("Making a choice~n"),
  Choice = rand:uniform(2),
  case Choice of
    1 -> {keep_state, Data};
    2 ->
      io:format("Sending ~p~n", [basic_cancel]),
      gen_consumer:send_basic_cancel(ChannelPid),
      {next_state, state9, Data}
  end;
state4(_EventType, _Msg, _Data) ->
  io:format("Unexpected event in state4, ignoring~n"),
  {keep_state, _Data}.

%% State 5: Handle bogus1, stop after sending
% -spec state5(atom(), {bogus1}, state_data()) -> {stop, normal, state_data()}.
state5(internal, {bogus1}, #state_data{channel_pid = ChannelPid} = Data) ->
  io:format("Sending ~p~n", [bogus1]),
  gen_consumer:send_bogus1(ChannelPid),
  {stop, normal, Data};
state5(_EventType, _Msg, _Data) ->
  io:format("Unexpected event in state5, ignoring~n"),
  {keep_state, _Data}.

%% State 7: Handle bogus2, stop after sending
% -spec state7(atom(), {bogus2}, state_data()) -> {stop, normal, state_data()}.
state7(internal, {bogus2}, #state_data{channel_pid = ChannelPid} = Data) ->
  io:format("Sending ~p~n", [bogus2]),
  gen_consumer:send_bogus2(ChannelPid),
  {stop, normal, Data};
state7(_EventType, _Msg, _Data) ->
  io:format("Unexpected event in state7, ignoring~n"),
  {keep_state, _Data}.

%% State 9: Handle basic_cancel_ok
% -spec state9(atom(), {pid(), {basic_cancel_ok}}, state_data()) -> {next_state, state10, state_data()}.
state9(cast, {ChannelPid, {basic_cancel_ok}}, #state_data{channel_pid = ChannelPid} = Data) ->
  {next_state, state10, Data};
state9(_EventType, _Msg, _Data) ->
  io:format("Unexpected event in state9, ignoring~n"),
  {keep_state, _Data}.

%%---------------------------------------------------------------------------
%% Helper Functions
%%---------------------------------------------------------------------------

%% Resolve consumer by tag or use default consumer
resolve_consumer(Tag, #state_data{consumers = Consumers, default_consumer = DefaultConsumer}) ->
  case maps:find(Tag, Consumers) of
    {ok, ConsumerPid} -> {consumer, ConsumerPid};
    error -> case DefaultConsumer of
               none -> error;
               _ -> {default, DefaultConsumer}
             end
  end.

%% Monitoring functions for consumer processes
add_to_monitor_dict(Pid, Monitors) ->
  case maps:find(Pid, Monitors) of
    error -> maps:put(Pid, {1, erlang:monitor(process, Pid)}, Monitors);
    {ok, {Count, MRef}} -> maps:put(Pid, {Count + 1, MRef}, Monitors)
  end.

remove_from_monitor_dict(Pid, Monitors) ->
  case maps:get(Pid, Monitors) of
    {1, MRef} -> erlang:demonitor(MRef), maps:remove(Pid, Monitors);
    {Count, MRef} -> maps:put(Pid, {Count - 1, MRef}, Monitors)
  end.
