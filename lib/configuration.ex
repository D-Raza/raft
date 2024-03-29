
# distributed algorithms, n.dulay, 14 jan 2024
# raft, configuration parameters v2

defmodule Configuration do

# _________________________________________________________ node_init()
def node_init() do
  # get node arguments and spawn a process to exit node after max_time
  config =
  %{
    node_suffix:     Enum.at(System.argv, 0),
    raft_timelimit:  String.to_integer(Enum.at(System.argv, 1)),
    debug_level:     String.to_integer(Enum.at(System.argv, 2)),
    debug_options:   "#{Enum.at(System.argv, 3)}",
    n_servers:       String.to_integer(Enum.at(System.argv, 4)),
    n_clients:       String.to_integer(Enum.at(System.argv, 5)),
    params_function: :'#{Enum.at(System.argv, 6)}',
    start_function:  :'#{Enum.at(System.argv, 7)}',
  }

  if config.n_servers < 3 do
    Helper.node_halt("Raft is unlikely to work with fewer than 3 servers")
  end # if

  spawn(Helper, :node_exit_after, [config.raft_timelimit])

  config |> Map.merge(Configuration.params(config.params_function))
end # node_init

# _________________________________________________________ node_info()
def node_info(config, node_type, node_num \\ "") do
  Map.merge config,
  %{
    node_type:     node_type,
    node_num:      node_num,
    node_name:     "#{node_type}#{node_num}",
    node_location: Helper.node_string(),
    line_num:      0,  # for ordering output lines
  }
end # node_info

# _________________________________________________________ params :default ()
def params :default do
  %{
    n_accounts:              100,      # account numbers 1 .. n_accounts
    max_amount:              1_000,    # max amount moved between accounts in a single transaction

    client_timelimit:        60_000,   # clients stops sending requests after this time(ms)
    max_client_requests:     5000,     # maximum no of requests each client will attempt
    client_request_interval: 1,        # interval(ms) between client requests
    client_reply_timeout:    50,       # timeout(ms) for the reply to a client request

    election_timeout_range:  100..200, # timeout(ms) for election, set randomly in range
    append_entries_timeout:  10,       # timeout(ms) for the reply to a append_entries request

    monitor_interval:        1000,     # interval(ms) between monitor summaries

    crash_servers: %{		       # server_num => crash_after_time (ms), ..
      # 3 => 5_000,
      # 4 => 8_000,
    },

    crash_leaders_after:    3000,    # nil or time after which leaders will crash
  }
end # params :default

# add further params functions for your own tests and experiments

# _________________________________________________________ params :testing_XX
def params :crashy1 do
  Map.merge (params :default),
  %{
    crash_servers: %{		       # server_num => crash_after_time (ms), ..
      3 => 3_000,
      4 => 9_000,
    }
    # omitted
  }
end # params :crashy1

def params :crashy2 do
  Map.merge (params :default),
  %{
    crash_servers: %{		       # server_num => crash_after_time (ms), ..
      1 => 5_000,
      2 => 7_000,
      3 => 9_000,
      4 => 11_000,
    }
  }
end # params :crashy2

def params :crashy3 do
  Map.merge (params :default),
  %{
    crash_leaders_after:  5000,
  }
end # params :crashy3

def params :slow_client do
  Map.merge (params :default),
  %{
    client_request_interval: 100
    # omitted
  }
end # params :slow_client

def params :slower_election_timeout do
  Map.merge (params :default),
  %{
    election_timeout_range:  1000..2000
    # omitted
  }
end # params :slower_election_timeout

def params :faster_election_timeout do
  Map.merge (params :default),
  %{
    election_timeout_range:  50..150
    # omitted
  }
end # params :faster_election_timeout

def params :more_accounts_with_more_transactions do
  Map.merge (params :default),
  %{
    n_accounts:              1000,
    max_amount:              4_000
    # omitted
  }
end # params :more_accounts

def params :less_accounts_with_smaller_transactions do
  Map.merge (params :default),
  %{
    n_accounts:              20,
    max_amount:              100
    # omitted
  }
end # params :less_accounts


# etc ..

end # Configuration













"""
def read_host_map(file_name) do      # read map of hosts for fully distributed execution via ssh
  # Format of lines
  #    line = <nodenum> <hostname> pair
  # returns Map of nodenum to hostname

  stream = File.stream!(file_name) |> Stream.map(&String.split/1)

  for [first, second | _] <- stream, into: Map.new do
    { (first |> String.to_integer), second }
  end # for
end # read_host_map
"""
