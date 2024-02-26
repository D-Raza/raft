
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

  { :APPEND_ENTRIES_REQUEST, %{leader_term: leader_term, commit_index: commit_index, prev_index: prev_index, prev_term: prev_term, leader_entries: leader_entries, leader_pid: leader_pid}} ->
    Debug.message(server, "+state", "Heartbeat received with commit_index #{commit_index}", 1002)

    server
    |> Timer.restart_election_timer()
    |> AppendEntries.handle_ape_request(%{leader_term: leader_term, commit_index: commit_index, prev_index: prev_index, prev_term: prev_term, leader_entries: leader_entries, leader_pid: leader_pid})

  # Server is not a leader
  { :APPEND_ENTRIES_REPLY, %{ follower_pid: _follower_pid, follower_term: _follower_term, can_append_entries: _can_append_entries, follower_last_index: _follower_last_index }} when server.role != :LEADER ->
    server

  # Server is a leader
  { :APPEND_ENTRIES_REPLY, %{ follower_pid: follower_pid, follower_term: follower_term, can_append_entries: can_append_entries, follower_last_index: follower_last_index }} ->
    # If the follower has a larger term, step down
    server = if follower_term > server.curr_term do
      server |> Vote.stepdown(%{ term: follower_term })
    else
      server |> AppendEntries.handle_ape_reply(%{ follower_pid: follower_pid, follower_term: follower_term, can_append_entries: can_append_entries, follower_last_index: follower_last_index })
    end
    server

  { :APPEND_ENTRIES_TIMEOUT, %{term: term, followerP: follower_pid }} ->
    server
    |> AppendEntries.handle_ape_timeout(%{term: term, follower_pid: follower_pid})

  { :VOTE_REQUEST, %{term: term, candidate_pid: candidate_pid, candidate_num: candidate_num, candidate_last_log_term: candidate_last_log_term, candidate_last_log_index: candidate_last_log_index}} ->
      # IO.inspect(server, label: "Server state in VOTE_REQUEST:")
      server
      |> Debug.info("Vote request received by #{server.server_num} with role #{server.role}: (DEBUG)", 1002)
      |> Vote.handle_vote_request(%{term: term, candidate_pid: candidate_pid, candidate_num: candidate_num, candidate_last_log_term: candidate_last_log_term, candidate_last_log_index: candidate_last_log_index})

  { :VOTE_REPLY, %{term: term, voter: voter, vote_granted: vote_granted}} when server.role == :CANDIDATE ->
      # IO.inspect(server, label: "Server state in VOTE_REPLY:")
      server
      |> Debug.message("+state", "#{server.server_num} receives vote reply of #{vote_granted} from #{voter}", 1002)
      |> Vote.handle_vote_reply(%{term: term, voter: voter, vote_granted: vote_granted})

  { :VOTE_REPLY, %{term: term, voter: _voter, vote_granted: _vote_granted}} when server.role != :CANDIDATE ->
    if term > server.curr_term do
      server |> Vote.stepdown(%{term: term})
    else
      server
    end

  { :ELECTION_TIMEOUT, %{term: term, election: _election}} when term < server.curr_term -> # if election timeout message from old term, ignore it.
    server |> Debug.message("+state", "Ellection Timeout message arrives from old term", 1002)

  { :ELECTION_TIMEOUT, %{term: term, election: _election} } when term >= server.curr_term ->
    # Don't vote for yourself yet, broadcast to everyone including self
    # Accept it, then vote for yourself

      cond do
        server.role in [:FOLLOWER, :CANDIDATE] ->
          server
          |> Debug.info("#{server.role} server #{server.server_num} timedout", 998)
          |> State.role(:CANDIDATE)
          |> State.inc_term()
          |> State.voted_for(server.server_num)
          |> State.new_voted_by()
          |> State.add_to_voted_by(server.server_num)
          |> Timer.restart_election_timer()
          |> Debug.info("Follower server #{server.server_num} becomes candidate", 998)
          |> send_vote_requests_to_all()
          |> next()
        # :CANDIDATE ->
        #   server
        #   |> Debug.state("Candidate server #{server.server_num} timedout", 998)
        :LEADER ->
          server
          |> Debug.message("+state", "THIS SHOULD NOT HAPPEN: Leader received election timeout message", 998)
      end

  { :CLIENT_REQUEST, payload = %{cmd: _cmd, clientP: _clientP, cid: _cid}} ->
    server
    |> ClientRequest.handle_client_request(payload)

  { :DB_REPLY, :OK } -> server

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
      # sends append_entries_timeouts. and when it receives APETIM, then it votes for itself (if its a candidate)
      send s, {:VOTE_REQUEST, %{term: server.curr_term, candidate_pid: server.selfP, candidate_num: server.server_num, candidate_last_log_term: Log.last_term(server), candidate_last_log_index: Log.last_index(server)}}
    end
  end)

  server
end # send_vote_requests_to_all



end # Server
