%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% LSINF2345 -- Languages and algorithms for distributed appliactions %%%
%%% Project: Implementing a distributed transactional key-value store  %%%
%%% Simon GUSTIN and Loan SENS                                         %%%
%%% May 2018                                                           %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(client).

%% API
-export([start_assessment_behavior/3, start_read_behavior/2]).

% Clients allow us to evaluate the throughput of the system
% or to read a file and send commands to transaction managers

%%%%%%%%%%%%%%%%%%% Creator functions %%%%%%%%%%%%%%%%%%%%%%%%%
start_assessment_behavior(TM_pid_list, Max_requests, Probabilities_type_msg) ->
  Start_time = erlang:monotonic_time(),
  loop_assessment_behavior(TM_pid_list, Max_requests, Probabilities_type_msg),
  End_time = erlang:monotonic_time(),
  Throughput_per_second = compute_throughput(Max_requests, Start_time, End_time),
  Elapsed_microsec = erlang:convert_time_unit(End_time-Start_time, native, micro_seconds),
  io:format("Throughtput: ~p insertions per second (elapsed ~p microsec) => latency ~ps~n", [Throughput_per_second, Elapsed_microsec, 1/Throughput_per_second]),
  erlang:exit(finished).

start_read_behavior(TM_pid_list, Input_file_path) ->
  {Answer1, Answer2} = file:open(Input_file_path, [read]),
  case Answer1 of
    error -> io:format("Couldn't open file ~p: ~p~n", [Input_file_path, Answer2]);
    ok ->
      Device = Answer2,
      io:format("[Client] Opened file ~p~n", [Input_file_path]),
      loop_read_behavior(TM_pid_list, Device),
      file:close(Device)
  end.

compute_throughput(Nb_requests, Start_time, End_time) ->
  % Computes the throughput in requests per seconds ($Start_time and $End_time are in native units)
  Elapsed_microsec = erlang:convert_time_unit(End_time-Start_time, native, micro_seconds),
  % NB: $micro_seconds is deprecated in Erlang 19 but we use Erlang 18 (in Erlang 19 use $microsecond (without s))
  (Nb_requests / Elapsed_microsec) * 1000000.

%%%%%%%%%%%%%%%%%%%% Main loop for assessment behavior %%%%%%%%%%%%%%%%%%%%%%%%%%
loop_assessment_behavior(_, 0, _) ->
  io:fwrite("[Client] Finished~n");
loop_assessment_behavior(TM_pid_list, Nb_msg, Probabilities_type_msg) ->
  % Sends a message to a random transaction manager and waits for the answer, $Nb_msg times
  % The choice of the type of message is made with probabilities given inside the tuple $Probabilities_type_msg
  % The inserted keys will never be already present in the data store
  Random_TM_pid = choose_random_element(TM_pid_list),
  Random_action = choose_random_action(Probabilities_type_msg),
  case Random_action of
    update ->
%%      io:format("[Client] update~n"),
      Key = "key"++integer_to_list(rand:uniform(100)-1),
      Value = "val"++integer_to_list(Nb_msg),
      Random_TM_pid ! {self(), {update, Key, Value}};
    read ->
%%      io:format("[Client] read~n"),
      Keys = generate_random_keys("key"),
      Random_TM_pid ! {self(), {snapshot_read, Keys}};
    gc ->
%%      io:format("[Client] gc~n"),
      Random_TM_pid ! {self(), gc};
    sleep ->
%%      io:format("[Client] sleep~n"),
      timer:sleep(10),
      loop_assessment_behavior(TM_pid_list, Nb_msg, Probabilities_type_msg)
  end,

  receive
    _ -> loop_assessment_behavior(TM_pid_list, Nb_msg-1, Probabilities_type_msg)
  end.

%================ Utility functions for assessment behavior ================
choose_random_element(List) ->
  Random_nb = rand:uniform(length(List)),
  lists:nth(Random_nb, List).

choose_random_action({Probability_update, Probability_read, Probability_gc, Probability_sleep}) ->
  % Chooses a random action given the provided probabilities (in percent)
  Random_nb = rand:uniform(100),
  if
    Random_nb =< Probability_update -> update;
    Random_nb =< Probability_update+Probability_read -> read;
    Random_nb =< Probability_update+Probability_read+Probability_gc -> gc;
    Random_nb =< Probability_update+Probability_read+Probability_gc+Probability_sleep -> sleep;
    true -> update
  end.

generate_random_keys(Prefix) ->
  % Generates a list of random keys beginning with $Prefix
  generate_random_keys(Prefix, rand:uniform(100)-1, []).
generate_random_keys(_, 0, Acc) -> Acc;
generate_random_keys(Prefix, Nb_keys, Acc) ->
  generate_random_keys(Prefix, Nb_keys-1, [Prefix++integer_to_list(rand:uniform(100)) | Acc]).

%%%%%%%%%%%%%%%%%%% Main loop for file reading behavior %%%%%%%%%%%%%%%%%%%%%%%
loop_read_behavior(TM_pid_list, File_IODevice) ->
  % Reads messages from a $File_IODevice
  % and sends the corresponding commands to a random transaction manager in the list
  % then waits for the answer
  case io:get_line(File_IODevice, "") of
    eof -> io:fwrite("[Client] finished~n");

    Line ->
      First_char = hd(string:strip(Line,left,$ )),
      if
        First_char =:= $# -> % Ignore comment line
          loop_read_behavior(TM_pid_list, File_IODevice);
        true ->
          Terms = get_terms(Line),
          case Terms of
            {error, Error_info} ->
              io:fwrite("[Client] read invalid line from input file (~p): ~p, ignoring it~n", [Line, Error_info]);
            [Command|Arguments] ->
              case Command of
                up ->
                  if
                    length(Arguments) =/= 2 ->
                      io:fwrite("[Client] read invalid update command from input file (~p), ignoring it~n", [Terms]);
                    true ->
                      [Key,Value|_] = Arguments,
                      Random_TM_pid = choose_random_element(TM_pid_list),
                      Random_TM_pid ! {self(), {update, Key, Value}},

                      receive
                        {_, ok} ->
%%                          io:fwrite("[Client] updated key~n"),
                          io:fwrite("ok~n")
                      end
                  end;

                read ->
                  if
                    length(Arguments) =:= 0 ->
                      io:fwrite("[Client] read invalid read command from input file (~p), ignoring it~n", [Terms]);
                    true ->
                      Random_TM_pid = choose_random_element(TM_pid_list),
                      Random_TM_pid ! {self(), {snapshot_read, Arguments}},

                      receive
                        {_, Values} ->
%%                          io:fwrite("[Client] received values ~p~n", [Values]),
                          io:fwrite("~p~n", [Values])
                      end
                  end;

                sleep ->
                  if
                    length(Arguments) =/= 1 ->
                      io:fwrite("[Client] read invalid sleep command from input file (~p), ignoring it~n", [Terms]);
                    true ->
%%                      io:format("[Client] sleep for ~p milliseconds~n", Arguments),
                      io:fwrite("ok~n"),
                      timer:sleep(hd(Arguments))
                  end;

                  gc ->
                    Random_TM_pid = choose_random_element(TM_pid_list),
                    Random_TM_pid ! {self(), gc},
                    receive
                      {_, ok} -> io:fwrite("ok~n")
                    end;

                _ ->
                  io:fwrite("[Client] read invalid command from input file (~p), ignoring it~n", [Terms])
              end
          end,
          loop_read_behavior(TM_pid_list, File_IODevice)
      end
  end.

%============== Utility functions for file reading behavior =================
get_terms(String) ->
  % Lets Erlang scan $String as Erlang declarations
  % Returns the list of unpacked tokens or {error, error_info} if $String wasn't valid
  {Result, Tokens_list_or_error_info, _} = erl_scan:string(clean(String) ++ "."),
  case Result of
    error -> {error, Tokens_list_or_error_info};
    ok -> unpack(Tokens_list_or_error_info)
  end.

clean(String) ->
  % Removes the trailing new line in $String (both under *nixes and Windows)
  string:strip(string:strip(String,right,$\n),right,$\r).

unpack(Tokens) ->
  % Returns a list of terms unpacked from $Tokens (format {type, something, term})
  unpack_acc(Tokens, []).

unpack_acc([], Acc) -> lists:reverse(Acc);
unpack_acc([{dot, _}], Acc) -> lists:reverse(Acc); % Trailing dot (needed for Erlang to parse the line)
unpack_acc([{_,_,Term}|T], Acc) ->
  unpack_acc(T, [Term|Acc]).
