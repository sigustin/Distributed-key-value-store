%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% LSINF2345 -- Languages and algorithms for distributed appliactions %%%
%%% Project: Implementing a distributed transactional key-value store  %%%
%%% Simon GUSTIN and Loan SENS                                         %%%
%%% May 2018                                                           %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(data_partition).

%% API
-export([start/0]).

start() -> loop(dict:new()).

%%%%%%%%%%%%%%%%%%%%% Main loop %%%%%%%%%%%%%%%%%%%%%%%%%%%

loop(Db) ->
  receive
    %================== Update command =======================
    {From, {update, Key, Value}} ->
%%      io:format("[DP] received UPDATE (~p; ~p)~n", [Key, Value]),
      Most_recent_value = get_most_recent_value(Key, Db),
      if
        Value =/= Most_recent_value ->
          Updated_db = dict:append(Key, {os:timestamp(), Value}, Db),
          From ! {self(), ok},
          loop(Updated_db);
        true ->
%%          io:fwrite("[DP] No need to update~n"),
          From ! {self, ok},
          loop(Db)
      end;

    %================== Snapshot read command ========================
    {From, {snapshot_read, Time, Key}} ->
      % Sends back the value corresponding to key at time $Time
      % or $nil if $Key is not found (or wasn't set yet before that time)
%%      io:fwrite("[DP] received READ (~p)~n", [Key]),
      Found_values = dict:find(Key, Db),
      case Found_values of
        error ->
          % $Key is not in the database => return $nil
%%          io:format("[DP] ~p not found~n", [Key]),
          From ! {self(), {answer_read, Key, nil}},
          loop(Db);
        {ok, Values} ->
          % $Key is in the database
          Result = get_snapshot_value(Values, Time),
          case Result of
            none -> % Key was not set at time $Time
%%              io:format("[DP] ~p was not yet set at time asked~n", [Key]),
              From ! {self(), {answer_read, Key, nil_yet}};
            _ ->
%%              io:format("[DP] ~p => ~p~n", [Key, Result]),
              From ! {self(), {answer_read, Key, Result}}
          end,
          loop(Db)
      end;

    %================= Garbage collection command ========================
    {_, {gc, Earliest_timestamp}} ->
      % Remove everything in the database that has a timestamp =< $Earliest_timestamp
%%      io:fwrite("[DP] received GARBAGE COLLECTION request~n"),
      Updated_db = garbage_collect_db(Earliest_timestamp, Db),
      loop(Updated_db);

    {From, partition_size} ->
      Size = compute_size(Db),
      From ! {partition_size, Size};

    %================ Invalid command ========================
    {From, Msg} ->
      io:format("[DP] received INVALID (~p)~n", [Msg]),
      From ! {self(), "unhandled message"},
      loop(Db);

    %=============== Exit command =====================
    exit -> exit(normal)
  end.

compute_size(Db) ->
  Keys = dict:fetch_keys(Db),
  compute_size_acc(Keys, Db, 0).
compute_size_acc([], _, Acc) -> Acc;
compute_size_acc([Key|Rest_keys], Db, Acc) ->
  compute_size_acc(Rest_keys, Db, Acc+length(dict:fetch(Key, Db))).

%%%%%%%%%%%%%%%%%% Utility functions %%%%%%%%%%%%%%%%%%%%%%%%%

%================= Update functions ==========================
get_most_recent_value(Key, Db) ->
  % Returns the most recent value for $Key (or $none if $Key is not in $Db)
  Answer = dict:find(Key, Db),
  case Answer of
    error -> none;
    {ok, Pairs} ->
      {_, Last_value} = lists:nth(length(Pairs), Pairs),
      Last_value
  end.

%================ Snapshot read functions =======================
get_snapshot_value(Pairs, Time) ->
  % Wait for the snapshot to be available, then
  % Returns the value in $Values whose timestamp <= $Time and closest to $Time
  % If all values have a timestamp > $Time (key had no value yet at $Time), returns $none
  wait_until(Time),
  Best_pair = lists:foldl(
    fun({T,V}, Current_best) ->
      if
        Current_best =:= none andalso T =< Time -> {T,V};
        Current_best =:= none -> none; % T > Time
        true -> {Best_time, _} = Current_best,
          if
            T =< Time andalso T > Best_time -> {T,V};
            true -> Current_best
          end
      end
    end,
    none,
    Pairs
  ),
  case Best_pair of
    none -> none;
    _ -> element(2, Best_pair)
  end.

wait_until(Timestamp) ->
  % Waits until $Timestamp is reached
  Current_timestamp = os:timestamp(),
  if
    Current_timestamp >= Timestamp -> over;
    true ->
      Timeout_ms = 250,
      timer:sleep(Timeout_ms),
      wait_until(Timestamp)
  end.

%=============== Garbage collection functions =====================
garbage_collect_db(Earliest_timestamp, Db) ->
  % Removes every value in $Db that has a timestamp =< $Earliest_timestamp (unless it is the last one)
  % and returns the new database
  Keys = dict:fetch_keys(Db),
  garbage_collect_each_key(Keys, Earliest_timestamp, Db).

garbage_collect_each_key([], _, Db) -> Db;
garbage_collect_each_key([Key | Rest_keys], Earliest_timestamp, Db) ->
  Values_list = dict:fetch(Key, Db),
  New_values_list = filter_old_values(Values_list, Earliest_timestamp),
  New_db = dict:store(Key, New_values_list, Db),
  garbage_collect_each_key(Rest_keys, Earliest_timestamp, New_db).

filter_old_values(Old_values_list, Earliest_timestamp) ->
  % Filters out the values with timestamp =< $Earliest_timestamp in $Old_values_list except the last one
  % and returns the new values list
  % We cannot use dict:update because its function only takes the old value as an argument
  filter_old_values_acc(Old_values_list, Earliest_timestamp, []).
filter_old_values_acc([Most_recent_pair|[]], _, Acc) -> lists:reverse([Most_recent_pair | Acc]);
filter_old_values_acc([{Timestamp, Value} | Rest_old_values], Earliest_timestamp, Acc) ->
  if
    Timestamp =< Earliest_timestamp -> filter_old_values_acc(Rest_old_values, Earliest_timestamp, Acc);
    true -> filter_old_values_acc(Rest_old_values, Earliest_timestamp, [{Timestamp, Value} | Acc])
  end.
