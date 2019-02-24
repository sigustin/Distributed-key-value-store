%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% LSINF2345 -- Languages and algorithms for distributed appliactions %%%
%%% Project: Implementing a distributed transactional key-value store  %%%
%%% Simon GUSTIN and Loan SENS                                         %%%
%%% May 2018                                                           %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(launcher).

%% API
-export([run_read_file/1, run_assessment/0]).

run_read_file(File_path) ->
  % Runs a client that reads the file at $File_path and issues commands accordingly
  timer:exit_after(timer:seconds(4), exit),
  TM_pid_list = make_data_store(2, 3),
  spawn(client, start_read_behavior, [TM_pid_list, File_path]),
  printMsg(TM_pid_list).

run_assessment() ->
  timer:exit_after(timer:seconds(4), exit),
  TM_pid_list = make_data_store(2, 3),
  spawn(client, start_assessment_behavior, [TM_pid_list, 100, {100, 0, 0, 0}]), % the tuple correspond to the percentage of chance that the current message issued by the manager is a {update, read, gc, sleep} message
  printMsg(TM_pid_list).

make_data_store(Nb_TM, Nb_partitions) ->
  % Creates $Nb_partitions data partitions and $Nb_TM started transaction managers
  % and returns the list of pid of transaction managers (we have access to the data partitions through them)
  Partitions_pid_list = make_partitions(Nb_partitions, []),
  TM_pid_list = make_TM(Nb_TM, []),
  lists:foreach(fun(TM_pid) -> TM_pid ! {start, TM_pid_list, Partitions_pid_list} end, TM_pid_list),
  TM_pid_list.

make_partitions(0, Acc) -> Acc;
make_partitions(Nb_partitions, Acc) ->
  New_partition_pid = spawn(data_partition, start, []),
  make_partitions(Nb_partitions-1, [New_partition_pid|Acc]).

make_TM(0, Acc) -> Acc;
make_TM(Nb_TM, Acc) ->
  % Creates the transaction managers of the data store (they are not started yet)
  make_TM(Nb_TM-1, [spawn(transaction_manager, create, []) | Acc]).

spawn_clients(_, 0, _) -> ok;
spawn_clients(TM_pid_list, Nb_clients, Nb_msg_to_send) ->
  % Spawns $Nb_clients clients
  spawn(client, start_assessment_behavior, [TM_pid_list, Nb_msg_to_send, {70,30,0, 0}]),
  spawn_clients(TM_pid_list, Nb_clients-1, Nb_msg_to_send).

printMsg(Pid_list) ->
  receive
    {'EXIT',_,exit} ->
      lists:foreach(fun(Pid) -> Pid ! exit end, Pid_list),
      io:fwrite("Finished: data store terminated~n");
    {_, Msg} ->
      io:format("Received: ~p~n", [Msg]),
      printMsg(Pid_list)
  end.
