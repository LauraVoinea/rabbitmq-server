% %% This Source Code Form is subject to the terms of the Mozilla Public
% %% License, v. 2.0. If a copy of the MPL was not distributed with this
% %% file, You can obtain one at https://mozilla.org/MPL/2.0/.
% %%
% %% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
% %%

% %% @doc A behaviour module for implementing consumers for
% %% amqp_channel. To specify a consumer implementation for a channel,
% %% use amqp_connection:open_channel/{2,3}.
% %% <br/>
% %% All callbacks are called within the gen_consumer process. <br/>
% %% <br/>
% %% See comments in amqp_gen_consumer.erl source file for documentation
% %% on the callback functions.
% %% <br/>
% %% Note that making calls to the channel from the callback module will
% %% result in deadlock.
% -module(amqp_gen_consumer).

% -include("amqp_client.hrl").

% -behaviour(gen_server2).

% -export([start_link/3, call_consumer/2, call_consumer/3, call_consumer/4]).
% -export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
%          handle_info/2, prioritise_info/3]).

% -record(state, {module,
%                 module_state}).


% -type ok_error() :: {ok, State :: term()} | {error, Reason:: term(), State :: term()}.

% %% This callback is invoked by the channel, when it starts
% %% up. Use it to initialize the state of the consumer. In case of
% -callback init(Args :: term()) -> {ok, State :: term()} | {stop, Reason :: term()} | ignore.

% %% This callback is invoked by the channel before a basic.consume
% -callback handle_consume(#'basic.consume'{}, Sender :: pid(), State :: term()) -> ok_error().

% %% This callback is invoked by the channel every time a
% %% basic.consume_ok is received from the server. Consume is the original
% %% method sent out to the server - it can be used to associate the
% %% call with the response.
% -callback handle_consume_ok(#'basic.consume_ok'{}, #'basic.consume'{}, State :: term()) -> ok_error().

% %% This callback is invoked by the channel every time a basic.cancel
% %% is sent to the server.
% -callback handle_cancel(#'basic.cancel'{}, State :: term()) -> ok_error().

% %% This callback is invoked by the channel every time a basic.cancel_ok
% %% is received from the server.
% -callback handle_cancel_ok(CancelOk :: #'basic.cancel_ok'{}, #'basic.cancel'{}, State :: term()) -> ok_error().

% %% This callback is invoked by the channel every time a basic.cancel
% %% is received from the server.
% -callback handle_server_cancel(#'basic.cancel'{}, State :: term()) -> ok_error().

% %% This callback is invoked by the channel every time a basic.deliver
% %% is received from the server.
% -callback handle_deliver(#'basic.deliver'{}, #amqp_msg{}, State :: term()) -> ok_error().

% %% This callback is invoked by the channel every time a basic.deliver
% %% is received from the server. Only relevant for channels that use
% %% direct client connection and manual flow control.
% -callback handle_deliver(#'basic.deliver'{}, #amqp_msg{}, DeliveryCtx :: {pid(), pid(), pid()}, State :: term()) -> ok_error().

% %% This callback is invoked the consumer process receives a
% %% message.
% -callback handle_info(Info :: term(), State :: term()) -> ok_error().

% %% This callback is invoked by the channel when calling
% %% amqp_channel:call_consumer/2. Reply is the term that
% %% amqp_channel:call_consumer/2 will return. If the callback
% %% returns {noreply, _}, then the caller to
% %% amqp_channel:call_consumer/2 and the channel remain blocked
% %% until gen_server2:reply/2 is used with the provided From as
% %% the first argument.
% -callback handle_call(Msg :: term(), From :: term(), State :: term()) ->
%     {reply, Reply :: term(), NewState :: term()} |
%     {noreply, NewState :: term()} |
%     {error, Reason :: term(), NewState :: term()}.

% %% This callback is invoked by the channel after it has shut down and
% %% just before its process exits.
% -callback terminate(Reason :: term(), State :: term()) -> any().

% %%---------------------------------------------------------------------------
% %% Interface
% %%---------------------------------------------------------------------------

% %% @type ok_error() = {ok, state()} | {error, reason(), state()}.
% %% Denotes a successful or an error return from a consumer module call.

% start_link(ConsumerModule, ExtraParams, Identity) ->
%     gen_server2:start_link(
%       ?MODULE, [ConsumerModule, ExtraParams, Identity], []).

% %% @spec (Consumer, Msg) -> ok
% %% where
% %%      Consumer = pid()
% %%      Msg = any()
% %%
% %% @doc This function is used to perform arbitrary calls into the
% %% consumer module.
% call_consumer(Pid, Msg) ->
%     gen_server2:call(Pid, {consumer_call, Msg}, amqp_util:call_timeout()).

% %% @spec (Consumer, Method, Args) -> ok
% %% where
% %%      Consumer = pid()
% %%      Method = amqp_method()
% %%      Args = any()
% %%
% %% @doc This function is used by amqp_channel to forward received
% %% methods and deliveries to the consumer module.
% call_consumer(Pid, Method, Args) ->
%     gen_server2:call(Pid, {consumer_call, Method, Args}, amqp_util:call_timeout()).

% call_consumer(Pid, Method, Args, DeliveryCtx) ->
%     gen_server2:call(Pid, {consumer_call, Method, Args, DeliveryCtx}, amqp_util:call_timeout()).

% %%---------------------------------------------------------------------------
% %% gen_server2 callbacks
% %%---------------------------------------------------------------------------

% init([ConsumerModule, ExtraParams, Identity]) ->
%     ?store_proc_name(Identity),
%     case ConsumerModule:init(ExtraParams) of
%         {ok, MState} ->
%             {ok, #state{module = ConsumerModule, module_state = MState}};
%         {stop, Reason} ->
%             {stop, Reason};
%         ignore ->
%             ignore
%     end.

% prioritise_info({'DOWN', _MRef, process, _Pid, _Info}, _Len, _State) -> 1;
% prioritise_info(_, _Len, _State)                                     -> 0.

% consumer_call_reply(Return, State) ->
%     case Return of
%         {ok, NewMState} ->
%             {reply, ok, State#state{module_state = NewMState}};
%         {error, Reason, NewMState} ->
%             {stop, {error, Reason}, {error, Reason},
%              State#state{module_state = NewMState}}
%     end.

% handle_call({consumer_call, Msg}, From,
%             State = #state{module       = ConsumerModule,
%                            module_state = MState}) ->
%     case ConsumerModule:handle_call(Msg, From, MState) of
%         {noreply, NewMState} ->
%             {noreply, State#state{module_state = NewMState}};
%         {reply, Reply, NewMState} ->
%             {reply, Reply, State#state{module_state = NewMState}};
%         {error, Reason, NewMState} ->
%             {stop, {error, Reason}, {error, Reason},
%              State#state{module_state = NewMState}}
%     end;
% handle_call({consumer_call, Method, Args}, _From,
%             State = #state{module       = ConsumerModule,
%                            module_state = MState}) ->
%     Return =
%         case Method of
%             #'basic.consume'{} ->
%                 ConsumerModule:handle_consume(Method, Args, MState);
%             #'basic.consume_ok'{} ->
%                 ConsumerModule:handle_consume_ok(Method, Args, MState);
%             #'basic.cancel'{} ->
%                 case Args of
%                     none -> %% server-sent
%                         ConsumerModule:handle_server_cancel(Method, MState);
%                     Pid when is_pid(Pid) -> %% client-sent
%                         ConsumerModule:handle_cancel(Method, MState)
%                 end;
%             #'basic.cancel_ok'{} ->
%                 ConsumerModule:handle_cancel_ok(Method, Args, MState);
%             #'basic.deliver'{} ->
%                 ConsumerModule:handle_deliver(Method, Args, MState)
%         end,
%     consumer_call_reply(Return, State);

% %% only supposed to be used with basic.deliver
% handle_call({consumer_call, Method = #'basic.deliver'{}, Args, DeliveryCtx}, _From,
%             State = #state{module       = ConsumerModule,
%                            module_state = MState}) ->
%     Return = ConsumerModule:handle_deliver(Method, Args, DeliveryCtx, MState),
%     consumer_call_reply(Return, State).

% handle_cast(_What, State) ->
%     {noreply, State}.

% handle_info(Info, State = #state{module_state = MState,
%                                  module       = ConsumerModule}) ->
%     case ConsumerModule:handle_info(Info, MState) of
%         {ok, NewMState} ->
%             {noreply, State#state{module_state = NewMState}};
%         {error, Reason, NewMState} ->
%             {stop, {error, Reason}, State#state{module_state = NewMState}}
%     end.

% terminate(Reason, #state{module = ConsumerModule, module_state = MState}) ->
%     ConsumerModule:terminate(Reason, MState).

% code_change(_OldVsn, State, _Extra) ->
%     {ok, State}.

% ======

-module(amqp_gen_consumer).
-behaviour(gen_statem).

% -include_lib("rabbit_common/include/rabbit.hrl").
% -include_lib("rabbit_common/include/rabbit_framing.hrl").

% Import AMQP records
-include("amqp_client.hrl").

-export([start_link/2, callback_mode/0, init/1, terminate/3]).

-export([send_bogus1/1, send_bogus2/1, send_basic_cancel/1, send_basic_consume/1, state1/3, state2/3, state3/3, state4/3, state5/3, state7/3, state9/3]).

-record(state_data, {
    channel_pid = undefined, 
    consumers = #{}, 
    default_consumer = none, 
    monitors = #{}, 
    mc_counter_1 = 0 :: integer(),
    amqp_module = undefined,   % Track the AMQP module
    amqp_state = undefined     % Track the AMQP module's state
}).

-type state_data() :: #state_data{}.

-callback state9(atom(),{pid(), {basic_cancel_ok}}, state_data()) -> {stop, normal, state_data()}.
-callback state7(atom(),{bogus2}, state_data()) -> {stop, normal, state_data()}.
-callback state5(atom(),{bogus1}, state_data()) -> {stop, normal, state_data()}.
-callback state4(atom(),{pid(), {basic_deliver}} | {pid(), {default_consumer_deliver}} |  basic_cancel_choice, state_data()) -> {next_state, state5, state_data()} | {next_state, state7, state_data()} | {next_state, state9, state_data()}.
-callback state3(atom(),{pid(), {temp}} | {pid(), {basic_cancel}}, state_data()) -> {next_state, state4, state_data()} | {stop, normal, state_data()}.
-callback state2(atom(),{pid(), {basic_consume_ok}}, state_data()) -> {next_state, state3, state_data()}.
-callback state1(atom(),{basic_consume}, state_data()) -> {next_state, state2, state_data()}.

-spec start_link(module(), list()) -> {ok, pid()} | {error, any()}.
start_link(CallbackModule, Args) ->
    case code:ensure_loaded(CallbackModule) of
        {module, CallbackModule} ->
            gen_statem:start_link({local, CallbackModule}, gen_consumer, {CallbackModule, Args}, []);
        {error, Reason} ->
            {error, Reason}
    end.

callback_mode() ->
    state_functions.

% -spec init({module(), list()}) -> {ok, state1, state_data()}.
init({CallbackModule, Args}) ->
    io:format("gen_consumer: Initializing with callback module ~p~n", [CallbackModule]),
    % Initialize the AMQP consumer module (amqp_gen_consumer)
    {ok, AmqpState} = amqp_gen_consumer:init(Args),
    put(callback_module, CallbackModule),
    {ok, state1, #state_data{amqp_module = amqp_gen_consumer, amqp_state = AmqpState}}.

% -spec send_bogus1(pid()) -> ok.
send_bogus1(ChannelPid) -> 
    gen_statem:cast(ChannelPid, {self(), {bogus1}}).
 
% -spec send_bogus2(pid()) -> ok.
send_bogus2(ChannelPid) -> 
    gen_statem:cast(ChannelPid, {self(), {bogus2}}).
 
% -spec send_basic_cancel(pid()) -> ok.
send_basic_cancel(ChannelPid) -> 
    gen_statem:cast(ChannelPid, {self(), {basic_cancel}}).
 
% -spec send_basic_consume(pid()) -> ok.
send_basic_consume(ChannelPid) -> 
    gen_statem:cast(ChannelPid, {self(), {basic_consume}}).

% -spec state1(atom(), {basic_consume}, state_data()) -> {next_state, state2, state_data()}.
state1(_EventType, {basic_consume}, Data) ->
    % CallbackModule = get(callback_module),
    AmqpModule = Data#state_data.amqp_module,
    AmqpState = Data#state_data.amqp_state,
    % Handle the basic.consume event in the amqp_gen_consumer
    {ok, NewAmqpState} = AmqpModule:handle_consume(#'basic.consume'{}, self(), AmqpState),
    {next_state, state2, Data#state_data{amqp_state = NewAmqpState}}.

% -spec state2(atom(), {pid(), {basic_consume_ok}}, state_data()) -> {next_state, state3, state_data()}.
state2(_, {_, {basic_consume_ok}, Counter}, #state_data{mc_counter_1 = MC} = Data) when Counter < MC ->
    {keep_state, Data};
state2(EventType, {ChannelPid, {basic_consume_ok}, _}, Data) ->
    CallbackModule = get(callback_module),
    AmqpModule = Data#state_data.amqp_module,
    AmqpState = Data#state_data.amqp_state,
    % Handle the basic.consume_ok event in the amqp_gen_consumer
    {ok, NewAmqpState} = AmqpModule:handle_consume_ok(#'basic.consume_ok'{}, #'basic.consume'{}, AmqpState),
    CallbackModule:state2(EventType, {ChannelPid, {basic_consume_ok}}, Data#state_data{amqp_state = NewAmqpState}).

% -spec state3(atom(), {pid(), {temp}} | {pid(), {basic_cancel}}, state_data()) -> {next_state, state4, state_data()} | {stop, normal, state_data()}.
state3(EventType, {ChannelPid, {temp}, _}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state3(EventType, {ChannelPid, {temp}}, Data);
state3(EventType, {ChannelPid, {basic_cancel}, _}, Data) ->
    CallbackModule = get(callback_module),
    AmqpModule = Data#state_data.amqp_module,
    AmqpState = Data#state_data.amqp_state,
    % Handle the basic.cancel event in the amqp_gen_consumer
    {ok, NewAmqpState} = AmqpModule:handle_cancel(#'basic.cancel'{}, AmqpState),
    CallbackModule:state3(EventType, {ChannelPid, {basic_cancel}}, Data#state_data{amqp_state = NewAmqpState}).

% -spec state4(atom(), {pid(), {basic_deliver}} | {pid(), {default_consumer_deliver}} |  basic_cancel_choice, state_data()) -> {next_state, state5, state_data()} | {next_state, state7, state_data()} | {next_state, state9, state_data()}.
state4(EventType, {ChannelPid, {basic_deliver}, #amqp_msg{} = Msg}, Data) ->
    CallbackModule = get(callback_module),
    AmqpModule = Data#state_data.amqp_module,
    AmqpState = Data#state_data.amqp_state,
    % Handle the basic.deliver event in the amqp_gen_consumer
    {ok, NewAmqpState} = AmqpModule:handle_deliver(#'basic.deliver'{}, Msg, AmqpState),
    CallbackModule:state4(EventType, {ChannelPid, {basic_deliver}, Msg}, Data#state_data{amqp_state = NewAmqpState});
state4(EventType, {ChannelPid, {default_consumer_deliver}, _}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state4(EventType, {ChannelPid, {default_consumer_deliver}}, Data);
state4(EventType, basic_cancel_choice, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state4(EventType, basic_cancel_choice, Data).

% -spec state5(atom(), {bogus1}, state_data()) -> {stop, normal, state_data()}.
state5(EventType, {bogus1}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state5(EventType, {bogus1}, Data).

% -spec state7(atom(), {bogus2}, state_data()) -> {stop, normal, state_data()}.
state7(EventType, {bogus2}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state7(EventType, {bogus2}, Data).

% -spec state9(atom(), {pid(), {basic_cancel_ok}}, state_data()) -> {stop, normal, state_data()}.
state9(EventType, {ChannelPid, {basic_cancel_ok}}, Data) ->
    CallbackModule = get(callback_module),
    AmqpModule = Data#state_data.amqp_module,
    AmqpState = Data#state_data.amqp_state,
    % Handle the basic.cancel_ok event in the amqp_gen_consumer
    {ok, NewAmqpState} = AmqpModule:handle_cancel_ok(#'basic.cancel_ok'{}, AmqpState),
    CallbackModule:state9(EventType, {ChannelPid, {basic_cancel_ok}}, Data#state_data{amqp_state = NewAmqpState}).

terminate(_, _, _) ->
    io:format("Terminating the gen_consumer process~n").