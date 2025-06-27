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
%% gen_consumer.erl
%% This module replaces amqp_gen_consumer using gen_statem with state_functions.
-behaviour(gen_statem).

%% Exported functions
-export([start_link/2, callback_mode/0, init/1, terminate/3, code_change/3]).
-export([call_consumer/2, call_consumer/3, call_consumer/4]).
-export([send_processing_complete/1, send_basic_cancel/1, send_basic_consume/1, send_register_default_consumer/1]).
-export([state1/3, state2/3, state3/3, state4/3, state5/3, state6/3, state8/3, state11/3]).

%% Include necessary headers
-include_lib("amqp_client.hrl").

%% Type definitions
% -define(STATE_DATA, state_data).
-record(state_data, {
    channel_pid  :: pid(),
    callback_module :: module()
}).

-type state_data() :: #state_data{}.

%% Start link function
-spec start_link(module(), list()) -> {ok, pid()} | {error, any()}.
start_link(CallbackModule, Args) ->
    % gen_statem:start_link({local, CallbackModule}, ?MODULE, {CallbackModule, Args}, []).
    case code:ensure_loaded(CallbackModule) of
        {module, CallbackModule} ->
            gen_statem:start_link({local, CallbackModule}, amqp_gen_consumer, {CallbackModule, Args}, []);
        {error, Reason} ->
            {error, Reason}
    end.


%% Callback mode
callback_mode() ->
    state_functions.

%% @spec (Consumer, Msg) -> ok
%% where
%%      Consumer = pid()
%%      Msg = any()
%%
%% @doc This function is used to perform arbitrary calls into the
%% consumer module.
call_consumer(Pid, Msg) ->
    gen_server2:call(Pid, {consumer_call, Msg}, amqp_util:call_timeout()).

%% @spec (Consumer, Method, Args) -> ok
%% where
%%      Consumer = pid()
%%      Method = amqp_method()
%%      Args = any()
%%
%% @doc This function is used by amqp_channel to forward received
%% methods and deliveries to the consumer module.
call_consumer(Pid, Method, Args) ->
    gen_server2:call(Pid, {consumer_call, Method, Args}, amqp_util:call_timeout()).

call_consumer(Pid, Method, Args, DeliveryCtx) ->
    gen_server2:call(Pid, {consumer_call, Method, Args, DeliveryCtx}, amqp_util:call_timeout()).


%% Init function
-spec init({module(), list()}) -> {ok, atom(), state_data()}.
init({CallbackModule, _Args}) ->
    io:format("gen_consumer: Initializing with callback module ~p~n", [CallbackModule]),
    %% Initialize state data without waiting for processes
    % ChannelPid = erlang:self(), %% Placeholder: Replace with actual channel PID
    % StateData = #state_data{
    %     channel_pid = ChannelPid,
    %     callback_module = CallbackModule
    % },
    % {ok, state1, StateData}.
    io:format("gen_consumer: Initializing with callback module ~p~n", [CallbackModule]),
    put(callback_module, CallbackModule),
    CallbackModule:init([]). 


%% Send functions
-spec send_processing_complete(pid()) -> ok.
send_processing_complete(ChannelPid) ->
    gen_statem:cast(ChannelPid, {self(), {processing_complete}}).

-spec send_basic_cancel(pid()) -> ok.
send_basic_cancel(ChannelPid) ->
    gen_statem:cast(ChannelPid, {self(), {basic_cancel}}).

-spec send_basic_consume(pid()) -> ok.
send_basic_consume(ChannelPid) ->
    gen_statem:cast(ChannelPid, {self(), {basic_consume}}).

-spec send_register_default_consumer(pid()) -> ok.
send_register_default_consumer(ChannelPid) ->
    gen_statem:cast(ChannelPid, {self(), {register_default_consumer}}).

%% State functions

%%% State1: Initial state
-spec state1(atom(), {register_default_consumer}, state_data()) -> 
    {next_state, state2, state_data()} | 
    {next_state, state2, state_data(), [term()]}.
state1(EventType, {register_default_consumer}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state1(EventType, {register_default_consumer}, Data).

% should match up with handle_consume
% send basic.consume to the server
% need Sender pid in here as well
-spec state2(atom(), {basic_consume}, state_data()) -> 
    {next_state, state3, state_data()}.
state2(EventType, {basic_consume}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state2(EventType, {basic_consume}, Data).

% basic.consume_ok response is received from the server.
-spec state3(atom(), {pid(), {basic_consume_ok}}, state_data()) -> 
    {next_state, state4, state_data()} | 
    {next_state, state4, state_data(), [term()]}.
state3(cast, {ChannelPid, {basic_consume_ok}}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state3(cast, {ChannelPid, {basic_consume_ok}}, Data).

-spec state4(atom(), {pid(), {process_message}} | 
    {pid(), {basic_cancel}} |  basic_cancel_choice |  
    {_ChannelPid, {basic_deliver}} |  
    {_ChannelPid, {basic_cancel_ok}}, state_data()) -> 
        {next_state, state5, state_data()} | 
        {next_state, state5, state_data(), [term()]} | 
        {stop, normal, state_data()} | 
        {next_state, state11, state_data()} | 
        {keep_state, state_data()}.
state4(EventType, {ChannelPid, {process_message}}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state4(cast, {ChannelPid, {process_message}}, Data);
state4(EventType, {ChannelPid, {basic_cancel}}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state4(cast, {ChannelPid, {basic_cancel}}, Data);
state4(EventType, basic_cancel_choice, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state4(EventType, basic_cancel_choice, Data);
state4(_EventType, {_ChannelPid, {basic_deliver}}, Data) ->
    % Discard outdated message
    {keep_state, Data};
state4(_EventType, {_ChannelPid, {basic_cancel_ok}}, Data) ->
    % Discard outdated message
    {keep_state, Data}.

-spec state5(atom(), {pid(), {basic_deliver}} |  
    basic_cancel_choice |  
    {_ChannelPid, {process_message}} |  
    {_ChannelPid, {basic_cancel_ok}}, state_data()) -> 
        {next_state, state6, state_data()} | 
        {next_state, state6, state_data(), [term()]} | 
        {next_state, state8, state_data()} | 
        {keep_state, state_data()}.
state5(cast, {ChannelPid, {basic_deliver}}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state5(cast, {ChannelPid, {basic_deliver}}, Data);
state5(EventType, basic_cancel_choice, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state5(EventType, basic_cancel_choice, Data);
state5(_EventType, {_ChannelPid, {process_message}}, Data) ->
    % Discard outdated message
    {keep_state, Data};
state5(_EventType, {_ChannelPid, {basic_cancel_ok}}, Data) ->
    % Discard outdated message
    {keep_state, Data}.

-spec state6(atom(), {processing_complete} | 
    {_ChannelPid, {process_message}} | 
    {_ChannelPid, {basic_deliver}} |  
    {_ChannelPid, {basic_cancel}} |  
    {_ChannelPid, {basic_cancel_ok}}, state_data()) -> 
        {next_state, state4, state_data()} | 
        {next_state, state4, state_data(), [term()]} | 
        {keep_state, state_data()}.
state6(EventType, {processing_complete}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state6(EventType, {processing_complete}, Data);
state6(_EventType, {_ChannelPid, {process_message}}, Data) ->
    % Discard outdated message
    {keep_state, Data};
state6(_EventType, {_ChannelPid, {basic_deliver}}, Data) ->
    % Discard outdated message
    {keep_state, Data};
state6(_EventType, {_ChannelPid, {basic_cancel}}, Data) ->
    % Discard outdated message
    {keep_state, Data};
state6(_EventType, {_ChannelPid, {basic_cancel_ok}}, Data) ->
    % Discard outdated message
    {keep_state, Data}.

-spec state8(atom(), {pid(), {basic_cancel_ok}} |  
    {_ChannelPid, {process_message}} |  
    {_ChannelPid, {basic_deliver}} |  
    {_ChannelPid, {basic_cancel}}, state_data()) -> 
        {stop, normal, state_data()} | 
        {keep_state, state_data()}.
state8(cast, {ChannelPid, {basic_cancel_ok}}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state8(cast, {ChannelPid, {basic_cancel_ok}}, Data);
state8(_EventType, {_ChannelPid, {process_message}}, Data) ->
    % Discard outdated message
    {keep_state, Data};
state8(_EventType, {_ChannelPid, {basic_deliver}}, Data) ->
    % Discard outdated message
    {keep_state, Data};
state8(_EventType, {_ChannelPid, {basic_cancel}}, Data) ->
    % Discard outdated message
    {keep_state, Data}.

-spec state11(atom(), {pid(), {basic_cancel_ok}} |  
    {_ChannelPid, {process_message}} |  
    {_ChannelPid, {basic_deliver}} |  
    {_ChannelPid, {basic_cancel}}, state_data()) -> 
        {stop, normal, state_data()} | 
        {keep_state, state_data()}.
state11(cast, {ChannelPid, {basic_cancel_ok}}, Data) ->
    CallbackModule = get(callback_module),
    CallbackModule:state11(cast, {ChannelPid, {basic_cancel_ok}}, Data);
state11(_EventType, {_ChannelPid, {process_message}}, Data) ->
    % Discard outdated message
    {keep_state, Data};
state11(_EventType, {_ChannelPid, {basic_deliver}}, Data) ->
    % Discard outdated message
    {keep_state, Data};
state11(_EventType, {_ChannelPid, {basic_cancel}}, Data) ->
    % Discard outdated message
    {keep_state, Data}.

%% Terminate function
terminate(_Reason, _StateName, _Data) ->
    io:format("Terminating consumer ~p~n", [self()]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
