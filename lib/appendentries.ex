
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule AppendEntries do

def handle_ape_request(server, %{leader_term: leader_term, commit_index: commit_index, prev_index: prev_index, prev_term: prev_term, leader_entries: leader_entries, leader_pid: leader_pid}) do
  Debug.message(server, "+state", "#{server.server_num} received ape request", 1002)
  server = Timer.restart_election_timer(server)

  server = if leader_term > server.curr_term do # If server's term is less than leader's term, step down
    server
    |> Vote.stepdown(%{term: leader_term}) # terms are equal
    |> State.leaderP(leader_pid)
  else
    server
  end


  # TODO: BUM? Maybe not?
  server = if leader_term == server.curr_term do
    server
    |> State.leaderP(leader_pid)
  else
    server
  end

  if leader_term < server.curr_term do
    send leader_pid, {:APPEND_ENTRIES_REPLY, %{ follower_pid: server.selfP, follower_term: server.curr_term, can_append_entries: false, follower_last_index: -1 }} # TODO: BUM?, nil -> -1
    server
  else # leader_term == server.curr_term
    can_append_entries = prev_index == 0 || (prev_index <= Log.last_index(server) && Log.term_at(server, prev_index) == prev_term)
    {server, index} = if can_append_entries do
      store_entries(server, prev_index, leader_entries, commit_index)
    else
      {server, 0}
    end
    send leader_pid, { :APPEND_ENTRIES_REPLY, %{ follower_pid: server.selfP, follower_term: server.curr_term, can_append_entries: can_append_entries, follower_last_index: index }}
    server
    |> apply_commits_and_request_db()

  end
  server
end # handle_ape_request

def handle_ape_reply(server, %{ follower_pid: follower_pid, follower_term: follower_term, can_append_entries: can_append_entries, follower_last_index: follower_last_index }) do
  Debug.message(server, "+state", "Handle ape reply from #{inspect(follower_pid)}", 1002)
  server |> Debug.assert(follower_term <= server.curr_term, "follower_term <= server.curr_term")

  # follower_term <= server.curr_term
  # role == :LEADER
  server = if follower_term == server.curr_term do
    server = if can_append_entries do
      # entries up to follower_last_index are replicated, so update match_index
      server
      |> State.next_index(follower_pid, follower_last_index + 1)
      |> State.match_index(follower_pid, follower_last_index)
    else
      State.next_index(server, follower_pid, max(1, server.next_index[follower_pid] - 1))
    end
    if server.next_index[follower_pid] <= Log.last_index(server) do
      send_entries(server, follower_pid)
    else
      server
    end
    check_for_majority_commits(server)
  else
    check_for_majority_commits(server)
  end
  server
 end # handle_ape_reply

defp check_for_majority_commits(server) do
  Debug.assert(server, server.role == :LEADER, "MAJORITY HAS TO BE CHECKED BY LEADER")
  current_commit_index = server.commit_index
  uncommitted_entries = Log.get_entries(server, current_commit_index + 1..Log.last_index(server))
  server = Debug.info(server, "Uncommitted log entries: #{inspect(uncommitted_entries)}", 1002)

  new_commit_index = Enum.reduce_while(uncommitted_entries, current_commit_index + 1, fn _, next_index ->
    replica_count = calculate_replica_count(server, next_index)
    if replica_count >= server.majority do
      {:cont, next_index + 1}
    else
      {:halt, next_index}
    end
  end) - 1

  server = server
           |> State.commit_index(new_commit_index)
           |> apply_commits_and_request_db()

  newly_commited_entries = Log.get_entries(server, current_commit_index + 1..new_commit_index)
  for {_, entry} <- newly_commited_entries do
    send entry.request.clientP, { :CLIENT_REPLY, %{ cid: entry.request.cid, leaderP: server.selfP, reply: :OK}}
  end
  server
end

defp calculate_replica_count(server, index) do
  Enum.reduce(server.servers, 1, fn server_pid, replication_count ->
    if server.selfP != server_pid and Map.get(server.match_index, server_pid) >= index do
      replication_count + 1
    else
      replication_count
    end
  end)
end


defp apply_commits_and_request_db(server) do
  cond do
    server.last_applied == server.commit_index ->
      Debug.message(server, "+state", "#{server.role} AAAAA", 1002)
      server
      |> Debug.info("No new commits to apply", 1002)
    true -> # server.last_applied != server.commit_index
      # Apply commits
      Debug.message(server, "+state", "#{server.role} BBBBB", 988)
      entries_to_apply = Log.get_entries(server, server.last_applied+1..server.commit_index)
      Enum.each(entries_to_apply, fn {_, entry} ->
        send server.databaseP, { :DB_REQUEST, entry.request }
      end)
      # Update last_applied
      State.last_applied(server, server.commit_index)
  end
end

def handle_ape_timeout_BUM(server, %{term: term, followerP: follower_pid}) do
  server = case server.role do
    :LEADER when server.curr_term == term ->
      server |> send_entries(follower_pid)
    :CANDIDATE ->
      IO.puts("AppendEntries timeout, server is a #{server.role}")
      # server
      # TODO: BUM?FUCKING BUM
      server = Timer.restart_append_entries_timer(server, follower_pid)
      # %{term: term, candidate_pid: candidate_pid, candidate_num: candidate_num, candidate_last_log_term: candidate_last_log_term, candidate_last_log_index: candidate_last_log_index}
      send follower_pid, { :VOTE_REQUEST, %{term: server.curr_term, candidate_pid: server.selfP, candidate_last_log_term: Log.last_term(server), candidate_last_log_index: Log.last_index(server)}}
      server
    :FOLLOWER ->
      server
  end
  server
end # handle_ape_timeout

def handle_ape_timeout(server, %{term: term, follower_pid: follower_pid}) do
  # IO.puts("AppendEntries timeout")
  server = case server.role do
    :LEADER when server.curr_term == term ->
      server |> send_entries(follower_pid)
    _ -> server
  end
  server
end # handle_ape_timeout

def send_entries_to_all_but_self(server) do
  # Enum.reduce(server.servers, server, fn follower_pid, acc_server ->
  #   if follower_pid != server.selfP do
  #     send_entries(acc_server, follower_pid)
  #   else
  #     acc_server
  #   end
  # end)
  followers = Enum.filter(server.servers, fn follower_pid -> follower_pid != server.selfP end)
  Enum.reduce(followers, server, fn follower_pid, acc_server -> send_entries(acc_server, follower_pid) end)
end # send_entries_to_all_but_self

defp send_entries(server, follower_pid) do
  # Determines whether to send new entries or a heartbeat based on follower's next index
  # next_index = server.next_index[follower_pid]
  # log_last_index = Log.last_index(server)
  # server =
  #   case next_index < log_last_index + 1 do
  #     # true  ->
  #     true ->
  #       # Calculate starting point for new entries and the term at the previous index
  #       follower_prev_index = next_index - 1
  #       entries = Log.get_entries(server, follower_prev_index + 1..log_last_index)
  #       prev_term = Log.term_at(server, follower_prev_index)

  #       # Send new entries to follower
  #       server = Timer.restart_append_entries_timer(server, follower_pid)
  #       send follower_pid, { :APPEND_ENTRIES_REQUEST, %{leader_term: server.curr_term, commit_index: server.commit_index, prev_index: follower_prev_index, prev_term: prev_term, leader_entries: entries, leader_pid: server.selfP}}
  #       server
  #     false ->
  #       # Follower is up to date, send a heartbeat
  #       server = Timer.restart_append_entries_timer(server, follower_pid) #SHOULD THESE BE ALWAYS 0? CHECK,INSTEAD SET commıt_ındex = commit_index ?
  #       send follower_pid, {:APPEND_ENTRIES_REQUEST, %{leader_term: server.curr_term, commit_index: 0, prev_term: 0, prev_index: 0, leader_entries: %{}, leader_pid: server.selfP}}
  #       server
  #   end
  # server
  server = Timer.restart_append_entries_timer(server, follower_pid)
  prev_index = server.next_index[follower_pid] - 1
  prev_term  = Log.term_at(server, prev_index)
  last_entry = min(Log.last_index(server), prev_index)
  entries = Log.get_entries(server, last_entry+1..Log.last_index(server))
  send follower_pid, { :APPEND_ENTRIES_REQUEST, %{leader_term: server.curr_term, commit_index: server.commit_index, prev_index: prev_index, prev_term: prev_term, leader_entries: entries, leader_pid: server.selfP}}
  server
end # send_entries

 defp store_entries(server, prev_log_index, entries, commit_index) do
  server =
    server
    |> Log.delete_entries(prev_log_index+1..Log.last_index(server))
    |> Log.merge_entries(entries)
    |> State.commit_index(min(commit_index, Log.last_index(server)))
  {server, Log.last_index(server)}
 end # store_entries

end # AppendEntries
