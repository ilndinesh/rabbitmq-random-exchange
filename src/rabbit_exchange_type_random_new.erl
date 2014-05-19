%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Consistent Hash Exchange.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2014 GoPivotal, Inc. All rights reserved.
%%

-module(rabbit_exchange_type_random_new).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).
-export([init/0]).

-rabbit_boot_step({?MODULE, [
  {description,   "exchange type random new"},
  {mfa,           {rabbit_registry, register, [exchange, <<"x-random-new">>, ?MODULE]}},
  {cleanup, {rabbit_registry, unregister, [exchange, <<"x-random-new">>]}},
  {requires,      rabbit_registry},
  {enables,       kernel_ready}
]}).

description() ->
    [{name, <<"x-random-new">>}, {description, <<"AMQP random exchange new. Like a direct exchange, but randomly chooses who to route to.">>}].

serialise_events() -> false.

route(#exchange{name = Name},
      #delivery{message = #basic_message{routing_keys = Routes}}) ->
    Matches = rabbit_router:match_routing_key(Name, Routes),
    %io:format("exchange: ~p~n", [X]),
    %io:format("delivery: ~p~n", [D]),
    %io:format("matches: ~p~n", [Matches]),
    case length(Matches) of
      Len when Len < 2 -> Matches;
      Len ->
        Rand = crypto:rand_uniform(1, Len + 1),
        [lists:nth(Rand, Matches)]
    end.

validate(_X) -> ok.
validate_binding(_X, _B) -> ok.
create(_Tx, _X) -> ok.
%recover(_X, _Bs) -> ok.
delete(_Tx, _X, _Bs) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Tx, _X, _B) -> ok.
remove_bindings(_Tx, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
