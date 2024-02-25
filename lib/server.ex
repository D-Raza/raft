
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Server do

# _________________________________________________________ Server.start()
def start(config, server_num) do

  config = config
    |> Configuration.node_info("Server", server_num)
    |> Debug.node_starting()

  receive do
  { :BIND, servers, databaseP } ->
    config
    |> State.initialise(server_num, servers, databaseP)
    |> Timer.restart_election_timer()
    |> Server.next()
  end # receive
end # start

# _________________________________________________________ next()
def next(server) do
  # invokes functions in AppendEntries, Vote, ServerLib etc

  server = receive do

  # Heartbeat request
  { :APPEND_ENTRIES_REQUEST, %{leader_pid: leader_pid, leader_term: leader_term, commit_index: _commit_index} } ->
    server =
      case server.role do
        :CANDIDATE ->
          cond do
            leader_term > server.curr_term ->
              updated_server =
                server
                |> Vote.stepdown(%{term: leader_term})
                |> State.leaderP(leader_pid)
                |> Debug.message("+state", "#{server.server_num} turned from candidate to follower after missing out the voting", 998)

              send leader_pid, { :APPEND_ENTRIES_REPLY, %{follower_pid: updated_server.selfP}}
              updated_server

            leader_term < server.curr_term ->
              server
              |> Debug.message("+state", "Leader term #{leader_term} is behind the candidate's term, which is #{server.curr_term}", 998)

            true ->
              updated_server =
                server
                |> Vote.stepdown(%{term: leader_term})
                |> State.leaderP(leader_pid)
                |> Debug.message("+state", "#{server.server_num} turned from candidate to follower", 998)

              send leader_pid, { :APPEND_ENTRIES_REPLY, %{follower_pid: updated_server.selfP} }
              updated_server
          end

        :FOLLOWER ->
          cond do
            leader_term > server.curr_term ->
              updated_server =
                server
                |> Debug.message("+state", "Follower #{server.server_num} missed out voting for new candidate", 998)
                |> Vote.stepdown(%{term: leader_term})
                |> State.leaderP(leader_pid)

              send leader_pid, { :APPEND_ENTRIES_REPLY, %{follower_pid: updated_server.selfP} }
              updated_server
            leader_term < server.curr_term ->
              server
              |> Debug.message("+state", "Follower #{server.server_num}'s term is greater than leader's term, which is #{leader_term}", 998)

            true ->
              updated_server =
                server
                |> Timer.restart_election_timer()

              send leader_pid, { :APPEND_ENTRIES_REPLY, %{follower_pid: updated_server.selfP} }
              updated_server
          end

        :LEADER ->
          server =
            server
            |> Debug.message("+state", "IMPOSSIBLE CASE")
          server
      end


    server
    |> Debug.info("Checking 1", 1002)

  # Heartbeat reply
  { :APPEND_ENTRIES_REPLY, %{follower_pid: _follower_pid} } ->
    # send follower_pid, {:APPEND_ENTRIES_REQUEST, %{leader_pid: server.selfP, leader_term: server.curr_term, commit_index: nil}}
    server
    |> Debug.info("Checking 2", 1002)

  { :APPEND_ENTRIES_REQUEST, %{leader_term: leader_term, commit_index: commit_index, prev_index: prev_index, prev_term: prev_term, leader_entries: leader_entries, leader_pid: leader_pid}} ->
    server
    |> Timer.restart_election_timer()
    |> AppendEntries.handle_ape_request(%{leader_term: leader_term, commit_index: commit_index, prev_index: prev_index, prev_term: prev_term, leader_entries: leader_entries, leader_pid: leader_pid})

  # Server is not a leader
  { :APPEND_ENTRIES_REPLY, %{ follower_pid: _follower_pid, follower_term: _follower_term, can_append_entries: _can_append_entries, follower_last_index: _follower_last_index }} when server.role != :LEADER ->
    server

  # Server is a leader
  { :APPEND_ENTRIES_REPLY, %{ follower_pid: follower_pid, follwer_term: follower_term, can_append_entries: can_append_entries, follower_last_index: follower_last_index }} ->
    # If the follower has a larger term, step down
    server = if follower_term > server.curr_term do
      server |> Vote.stepdown(%{ term: follower_term })
    else
      server |> AppendEntries.handle_ape_reply(%{ follower_pid: follower_pid, follower_term: follower_term, can_append_entries: can_append_entries, follower_last_index: follower_last_index })
    end
    server

  { :APPEND_ENTRIES_TIMEOUT, %{term: term, follower_pid: follower_pid }} ->
    server
    |> AppendEntries.handle_ape_timeout(%{term: term, follower_pid: follower_pid})

  { :VOTE_REQUEST, %{term: term, candidate_pid: candidate_pid, candidate_num: candidate_num}} ->
      # IO.inspect(server, label: "Server state in VOTE_REQUEST:")
      server
      |> Debug.state("Server state in VOTE_REQUEST: (DEBUG)", 1002)
      |> Vote.handle_vote_request(%{term: term, candidate_pid: candidate_pid, candidate_num: candidate_num})

  { :VOTE_REPLY, %{term: term, voter: voter, vote_granted: vote_granted}} when server.role == :CANDIDATE ->
      # IO.inspect(server, label: "Server state in VOTE_REPLY:")
      server
      |> Debug.state("Server state in VOTE_REPLY: (DEBUG)", 1002)
      |> Vote.handle_vote_reply(%{term: term, voter: voter, vote_granted: vote_granted})

  { :VOTE_REPLY, %{term: _term, voter: _voter, vote_granted: _vote_granted}} when server.role != :CANDIDATE ->
      server

  { :ELECTION_TIMEOUT, %{term: _term, election: _election} } ->
    # Don't vote for yourself yet, broadcast to everyone including self
    # Accept it, then vote for yourself

      case server.role do
        :FOLLOWER ->
          server
          |> Timer.restart_election_timer()
          |> Debug.info("#{server.server_num} follower timeout", 998)
          |> State.inc_term()
          |> State.role(:CANDIDATE)
          |> State.new_voted_by()
          |> State.voted_for(server.server_num)
          |> State.add_to_voted_by(server.server_num)
          |> Debug.info("#{server.server_num} follower becomes candidate", 998)
          |> send_vote_requests_to_all()
          |> next()
        :CANDIDATE ->
          server
          |> Debug.state("Candidate #{server.server_num} timedout", 998)
        # :LEADER ->

      end

  { :CLIENT_REQUEST, %{cmd: cmd, clientP: clientP, cid: cid}} ->
    server
    |> ClientRequest.handle_client_request(%{cmd: cmd, clientP: clientP, cid: cid})

  unexpected ->
    Helper.node_halt("***** Server: unexpected message #{inspect unexpected}")

  end # receive

  server |> next()
end # next


def send_vote_requests_to_all(server) do
  server
  |> Debug.info("Debug to see current term inside sending all requests", 1002)

  Enum.each(server.servers, fn s ->
    unless s == server.selfP do
      send s, {:VOTE_REQUEST, %{term: server.curr_term, candidate_pid: server.selfP, candidate_num: server.server_num}}
    end
  end)

  server
end # send_vote_requests_to_all



end # Server
