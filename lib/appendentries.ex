
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule AppendEntries do

 def handle_ape_request(server, %{leader_term: leader_term, commit_index: commit_index, prev_index: prev_index, prev_term: prev_term, leader_entries: leader_entries, leader_pid: leader_pid}) do
    # Case 1: If not a follower (i.e. leader or candidate), and receive an APE request with a larger term, step down
    server = case server.role do
      :LEADER when server.curr_term < leader_term ->
        server
        |> Vote.stepdown(%{term: leader_term})
        |> State.leaderP(leader_pid)
      :CANDIDATE when server.curr_term < leader_term ->
        server
        |> Vote.stepdown(%{term: leader_term})
        |> State.leaderP(leader_pid)
      _ -> server
    end

    # If server's term is greater than leader's term, reject leader
    if server.curr_term > leader_term do
      send server.leaderP, {:APPEND_ENTRIES_REPLY, %{ follower_pid: server.selfP, follower_term: server.curr_term, can_append_entries: false, follower_last_index: nil }}
    end

    # If server's term == leader's term, and prev_indexes prev_terms match, then we can append
    terms_match = server.curr_term == leader_term
    prev_indexes_and_terms_match = prev_index == 0 || (prev_index <= Log.last_index(server) && Log.term_at(server, prev_index) == prev_term)
    can_append_entries = terms_match and prev_indexes_and_terms_match

    server =
      if can_append_entries do
        # Append entries
        server |> update_log_entries(%{prev_index: prev_index, leader_entries: leader_entries, commit_index: commit_index})
      else
        server
      end

    if server.curr_term == leader_term do
      send server.leaderP, { :APPEND_ENTRIES_REPLY, %{ follower_pid: server.selfP, follower_term: server.curr_term, can_append_entries: can_append_entries, follower_last_index: server.commit_index }}
    end

  server
 end # handle_ape_request



def handle_ape_reply(server, %{ follower_pid: follower_pid, follower_term: follower_term, can_append_entries: can_append_entries, follower_last_index: follower_last_index }) do
  server |> Debug.assert(follower_term <= server.curr_term, "follower_term <= server.curr_term")

  server =
    if can_append_entries do
      # Increment next index for follower on successful append
      server
      |> State.next_index(follower_pid, follower_last_index + 1)
    else
      # Decrement next index for follower on failed append, ensuring it's not less than 1
      server
      |> State.next_index(follower_pid, max(1, server.next_index[follower_pid] - 1))
      |> send_entries(follower_pid)
    end

  # Constructs a counter to check if a majority of servers have committed to their log
  index_to_count =
    (server.last_applied + 1)..Log.last_index(server)
    |> Enum.reduce(Map.new(), fn index, acc ->
      count = Enum.reduce(server.servers, 1, fn server_pid, count ->
        if server.selfP != server_pid and server.next_index[server_pid] > index, do: count + 1, else: count
      end)
      Map.put(acc, index, count)
    end)

  # Iterate through counter to check for majority
  index_to_count
  |> Enum.each(fn {index, num_of_approvals} ->
    # Notify local db to store request if majority of servers have committed
    if num_of_approvals >= server.majority do
      # TODO: Do database functionality. For now, just print to console
      IO.puts("Majority of servers have committed to their log")
      send server.databaseP, { :DB_REQUEST, Log.request_at(server, index) }
    end
  end)

  server
 end # handle_ape_reply

def handle_ape_timeout(server, %{term: term, follower_pid: follower_pid}) do
  IO.puts("AppendEntries timeout")
  server = case server.role do
    :LEADER when server.curr_term == term ->
      server |> send_entries(follower_pid)
    _ -> server
  end
  server
end # handle_ape_timeout

def send_entries(server, follower_pid) do
  # Determines whether to send new entries or a heartbeat based on follower's next index
  next_index = server.next_index[follower_pid]
  log_last_index = Log.last_index(server)

  case next_index < log_last_index + 1 do
    true  ->
      # Calculate starting point for new entries and the term at the previous index
      follower_prev_index = next_index - 1
      entries = Log.get_entries(server, follower_prev_index + 1..log_last_index)
      prev_term = Log.term_at(server, follower_prev_index)

      # Send new entries to follower
      Timer.restart_append_entries_timer(server, follower_pid)
      send follower_pid, { :APPEND_ENTRIES_REQUEST, %{leader_term: server.curr_term, commit_index: server.commit_index, prev_index: follower_prev_index, prev_term: prev_term, leader_entries: entries, leader_pid: server.selfP}}

    false ->
      # Follower is up to date, send a "heartbeat"
      Timer.restart_append_entries_timer(server, follower_pid)
      send follower_pid, { :APPEND_ENTRIES_REQUEST,  %{leader_pid: server.selfP, leader_term: server.curr_term, commit_index: server.commit_index}}

  end

  server
end # send_entries

 defp update_log_entries(server, %{prev_index: _prev_index, leader_entries: leader_entries, commit_index: _commit_index}) do
    # Find the part in entries to begin appending to, i.e. the first entry that doesn't match
    divergence_point = Enum.find_value(leader_entries, fn {index, entry} ->
      if Log.last_index(server) >= index and server.log[index].term != entry.term do
        {:stop, index}
      end
    end) || :no_divergence

    # Adjust logic for handling entries based on the divergence point
    new_entries = case divergence_point do
      :no_divergence -> leader_entries
      _ -> Map.drop(leader_entries, Enum.to_list(0..divergence_point-1))
    end

    updated_server = case divergence_point do
      :no_divergence -> server
      _ ->
        if divergence_point < Log.last_index(server) do
          server
          |> Log.delete_entries_from(divergence_point)
        else
          server
        end
    end

    # Merge logs, and update commit_index
    updated_server = if new_entries != %{} do
      updated_server
      |> Log.merge_entries(new_entries)
      |> State.commit_index(Log.last_index(updated_server))
    else
      updated_server
    end

    updated_server
 end # update_log_entries

end # AppendEntries
