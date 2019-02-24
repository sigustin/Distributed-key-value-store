# Distributed-key-value-store
An implementation of a distributed transactional key-value store, using message-passing concurrency.
This project **simulates a distributed system (with different agents) that use a transactional key-value store**.
The store is distributed over a certain number of concurrent node and used by different concurrent agent.

Synchronization was implemented using a **timestamp and snapshot approach**.

This project was made in 2018 for the course LSINF2345 &ndash; Languages and Algorithms for Distributed Applications.

*Languages used:*
- *Erlang*

## Collaborators
This is a group project I made with Loan Sens.
However, as my collaborator could not used to Erlang, I made all the code while he designed parts of the architecture of the project.

## What I learned
- The Erlang programming language
- To make a distributed application using message-passing concurrency
- To avoid race conditions in a distributed system setting

## Files worth checking out
- The instructions: [Project description.pdf](https://github.com/sigustin/Distributed-key-value-store/blob/master/Project%20description.pdf)
- Our report explaining what we made: [LSINF2345_Project_Gustin-Sens_Report.pdf](https://github.com/sigustin/Distributed-key-value-store/blob/master/LSINF2345_Project_Gustin-Sens_Report.pdf)
- The client source code, sending request to the transactional key-value store: [src/client.erl](https://github.com/sigustin/Distributed-key-value-store/blob/master/src/client.erl)
- The representation of the data that is stored in the system: [src/data_partition.erl](https://github.com/sigustin/Distributed-key-value-store/blob/master/src/data_partition.erl)
- A node storing the key-value store: [src/transactional_manager.erl](https://github.com/sigustin/Distributed-key-value-store/blob/master/src/transaction_manager.erl)

## Compilation and execution
You will need the [Erlang compiler and interpreter (BVM)](https://www.erlang.org/) to execute this project.

Compile the project:
```sh
erlc src/*
```

Run the project:
```sh
erl
```
Then once the interpreter opens:
```erlang
c(launcher). % Load the launcher
launcher:run_read_file(input_files/test_input.txt). % Take command from 'test_input.txt' file
```
