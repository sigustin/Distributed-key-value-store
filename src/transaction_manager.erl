%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% LSINF2345 -- Languages and algorithms for distributed appliactions %%%
%%% Project: Implementing a distributed transactional key-value store  %%%
%%% Simon GUSTIN and Loan SENS                                         %%%
%%% May 2018                                                           %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(transaction_manager).

%% API
-export([create/0]).

create() ->
  % Simply allows to create the process and wait to receive a start message to start
  receive
    {start, TM_pid_list, Partitions_pid_list} ->
      Other_TM_pid_list = [Pid || Pid <- TM_pid_list, Pid =/= self()],
      io:format("starting TM: (TMs ~p; Partitions ~p)~n", [Other_TM_pid_list, Partitions_pid_list]),
      if
        length(Partitions_pid_list) =:= 0 -> erlang:error(no_data_partitions_error);
        true -> loop(Other_TM_pid_list, Partitions_pid_list)
      end
  end.

%%%%%%%%%%%%%%%%%%% Main loop %%%%%%%%%%%%%%%%%%%%%%%%%%%%

loop(TM_pid_list, Partitions_pid) ->
  receive
    %================ Update command ========================
    {From, {update, Key, Value}} ->
%%      io:format("[TM] received UPDATE (~p; ~p)~n", [Key, Value]),
      Responsible_partition_pid = get_responsible_partition(Key, Partitions_pid),
      Responsible_partition_pid ! {self(), {update, Key, Value}},
      % We don't wait for the answer in this case since it cannot fail (and it would decrease the throughput)
      From ! {self(), ok},
      loop(TM_pid_list, Partitions_pid);

    %--------------- Answer to update -----------------------
    {_, ok} ->
      % The previous update worked (waiting for this message would decrease the throughput)
      loop(TM_pid_list, Partitions_pid);

    %================= Snapshot read command ==================
    {From, {snapshot_read, Keys}} ->
%%      io:format("[TM] received READ (~p)~n", [Keys]),
      if
        length(Keys) =:= 0 ->
          From ! {self(), []},
          loop(TM_pid_list, Partitions_pid);
        true ->
          Current_timestamp = os:timestamp(),
          lists:foreach(
            fun(Key) ->
              Responsible_partition_pid = get_responsible_partition(Key, Partitions_pid),
              Responsible_partition_pid ! {self(), {snapshot_read, Current_timestamp, Key}}
            end,
            Keys
          ),
          Answers = wait_read_answers(length(Keys)),
          Sorted_answers = sort_answers(Keys, Answers),
          From ! {self(), Sorted_answers},
          loop(TM_pid_list, Partitions_pid)
      end;

    %================= Garbage collection command ===================
    {From, gc} ->
%%      io:format("[TM] received GARBAGE COLLECTION request~n"),
      Earliest_timestamp = get_earliest_timestamp(TM_pid_list),
      lists:foreach(fun(Partition_pid) -> Partition_pid ! {self(), {gc, Earliest_timestamp}} end, Partitions_pid),
      From ! {self(), ok},
      loop(TM_pid_list, Partitions_pid);

    %----------------- Timestamp query (for GC) ------------------------
    {From, timestamp_query} ->
      From ! {self(), {timestamp_answer, os:timestamp()}},
      loop(TM_pid_list, Partitions_pid);

    {From, store_size} ->
      Store_size = compute_store_size(Partitions_pid, 0),
      From ! {store_size, Store_size};

    %================= Invalid command ========================
    {From, Msg} ->
      io:format("[TM] received INVALID (~p)~n", [Msg]),
      From ! "unhandled message",
      loop(TM_pid_list, Partitions_pid);

    %=============== Exit command ==========================
    exit ->
      lists:foreach(fun(Pid) -> Pid ! exit end, Partitions_pid),
      erlang:exit(normal)
  end.

compute_store_size([], Acc) -> Acc;
compute_store_size([Partition_pid|Rest_partitions_pid], Acc) ->
  Partition_pid ! {self(), partition_size},
  receive
    {partition_size, Partition_size} -> compute_store_size(Rest_partitions_pid, Acc+Partition_size)
  end.

%%%%%%%%%%%%%%%%%%% Utility functions %%%%%%%%%%%%%%%%%%%%%%%%%%

get_responsible_partition(Key, Partitions_pid) ->
  % Returns the pid of the partition hosting $Key
  Responsible_partition_index = erlang:phash(Key, length(Partitions_pid)),
  lists:nth(Responsible_partition_index, Partitions_pid).

%================ Snapshot read functions =========================
wait_read_answers(Nb_answers) ->
  % Waits to receive $Nb_answers answers from the data partitions (after sending them read requests)
  % Returns all the answers as a list (in received order)
  wait_read_answers_acc(Nb_answers, []).
wait_read_answers_acc(0, Acc) -> Acc;
wait_read_answers_acc(Nb_answers, Acc) ->
  receive
    {_, {answer_read, Key, Ans}} ->
      wait_read_answers_acc(Nb_answers-1,  [{Key, Ans} | Acc])
  end.

sort_answers(Keys, Pairs) ->
  % Sorts the pairs in the same order as $Keys and returns the values only
  sort_answers_acc(Keys, Pairs, []).
sort_answers_acc([], _, Acc) -> lists:reverse(Acc);
sort_answers_acc([Key|Rest_keys], Pairs, Acc) ->
  Value = hd([V || {K,V} <- Pairs, K =:= Key]), % length =/= 1 if the same key is being read several times
  sort_answers_acc(Rest_keys, Pairs, [Value | Acc]).

%================= Garbage collection functions =========================
get_earliest_timestamp(TM_pid_list) ->
  % Returns the smallest timestamp between all the processes in $TM_pid_list and $self()
  lists:foreach(fun(Pid) -> Pid ! {self(), timestamp_query} end, TM_pid_list),
  wait_timestamp_answers(length(TM_pid_list), os:timestamp()).
wait_timestamp_answers(0, Earliest_timestamp) -> Earliest_timestamp;
wait_timestamp_answers(Nb_answers, Current_earliest_timestamp) ->
  receive
    {_, {timestamp_answer, Timestamp}} ->
      if
        Timestamp < Current_earliest_timestamp -> wait_timestamp_answers(Nb_answers-1, Timestamp);
        true -> wait_timestamp_answers(Nb_answers-1, Current_earliest_timestamp)
      end;
    {From, timestamp_query} -> % Needed to avoid a deadlock in case two garbage collections are asked at the same time to two TMs
      From ! {self(), {timestamp_answer, os:timestamp()}},
      wait_timestamp_answers(Nb_answers, Current_earliest_timestamp)
  end.
