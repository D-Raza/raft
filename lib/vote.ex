
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Vote do
  # Case 1: If request term is less than server's current term, reject vote
  def handle_vote_request(server, %{term: term, candidate_pid: candidate_pid, candidate_num: candidate_num, candidate_last_log_term: candidate_last_log_term, candidate_last_log_index: candidate_last_log_index}) do
    case server.role do
      :LEADER ->
        # TODO: consider network partitioning
        IO.puts("Leader case being hit in handle_vote_request, leader is #{server.server_num}")
        cond do
          term < server.curr_term ->
            server
            |> Debug.message("+state", "This case should not be hit", 998)
          term > server.curr_term ->
            server =
              server
              |> Debug.message("+state", "Leader #{server.server_num} did not send heartbeat", 998)
              |> Debug.message("+state", "Previous leader but now follower #{server.server_num} votes for #{candidate_num}", 998)
              |> stepdown(%{term: term})
              # |> State.voted_for(candidate_num)



            if server.voted_for in [nil, candidate_num] and (candidate_last_log_term > Log.last_term(server) or (candidate_last_log_term == Log.last_term(server) and candidate_last_log_index >= Log.last_index(server))) do
              server =
                server
                |> State.voted_for(candidate_num)
                |> Debug.message("+state", "Check after #{server.server_num} voted and before vote_reply sent", 1002)

              send candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: true}}
              server |> Debug.message("+state", "Check after #{server.server_num} voted and after vote_reply sent", 1002)
            else
              send candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: false}}
            end

          # send(candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: true}})
          server

          true -> # term == server.curr_term
            server
            |> Debug.message("+state", "Leader #{server.server_num} received votereq from #{candidate_num}", 998)
        end
      :FOLLOWER ->
        cond do
          term < server.curr_term ->
            send(candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: false}})
            server
          term > server.curr_term ->
            server =
              server
              #|> Debug.assert(is_nil(server.voted_for), "Voted_for must be nil? but is #{server.voted_for}")
              |> State.curr_term(term)
              |> Timer.restart_election_timer()

            Debug.assert(server, server.curr_term == term, "updated server's term should be equal to term")

            if server.voted_for in [nil, candidate_num] and (candidate_last_log_term > Log.last_term(server) or (candidate_last_log_term == Log.last_term(server) and candidate_last_log_index >= Log.last_index(server))) do
              server =
                server
                |> Debug.message("+state", "Follower #{server.server_num} votes for #{candidate_num}", 998)
                |> State.voted_for(candidate_num)
                 # cancel for debug, should be restart.

              send candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: true}}
            else
              send candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: false}}
            end

            server

          true -> # term == server.curr_term
            updated_server =
              server
              |> Timer.restart_election_timer()

            send(candidate_pid, {:VOTE_REPLY, %{term: updated_server.curr_term, voter: updated_server.server_num, vote_granted: false}})
            updated_server
        end
      :CANDIDATE ->
        cond do
          term < server.curr_term ->
            send(candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: false}})
            server
          term > server.curr_term ->
            server =
              server
              |> Debug.message("+state", "Candidate #{server.server_num} votes for #{candidate_num}", 998)
              |> stepdown(%{term: term})

            if server.voted_for in [nil, candidate_num] and (candidate_last_log_term > Log.last_term(server) or (candidate_last_log_term == Log.last_term(server) and candidate_last_log_index >= Log.last_index(server))) do
              server =
                server
                |> State.voted_for(candidate_num)

              send candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: true}}
              server
            else
              send candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: false}}
            end
          server
          true ->
            send(candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: false}})
            server
        end
    end
    #|> Debug.message("+state", "THIS SHOULD NOT HAVE A TERM OF 0")
  end # handle_vote_request


  def handle_vote_reply(server, %{term: term, voter: voter, vote_granted: vote_granted}) do
        # ONLY CANDIDATES CAN PROCESS VOTE REPLIES IN OUR IMPLEMENTATION
        cond do
          term > server.curr_term -> # Vote reply is from a new election, step down
            server
            |> Debug.message("+state", "Candidate #{server.server_num} received vote reply with higher term of #{term} from #{voter}", 998)
            |> stepdown(%{term: term})
          term < server.curr_term -> # Vote reply is from a previous election, ignore
            server
            |> Debug.message("+state", "This case should never hit, voter is #{voter} whose term is #{term}", 998)
          true ->
            updated_server =
              if vote_granted do
                server
                |> Debug.message("+state", "#{server.server_num} received vote from #{voter}", 998)
                |> State.add_to_voted_by(voter)
              else
                server
              end
            updated_server =
              if State.vote_tally(updated_server) >= server.majority do
                updated_server
                |> establish_leadership()
              else
                updated_server
              end
            updated_server
        end

  # server
  end # handle_vote_reply

def stepdown(server, %{term: term}) do
  server
  |> State.curr_term(term)
  |> State.role(:FOLLOWER)
  |> State.voted_for(nil)
  |> State.new_voted_by() # Only for leaders
  |> Timer.cancel_all_append_entries_timers() # Only for leaders
  |> Timer.restart_election_timer()
end # stepdown

defp establish_leadership(server) do
  server
  |> State.role(:LEADER)
  |> Debug.message("+state", "LEADER IS NOW #{server.server_num}", 998)
  |> Timer.cancel_election_timer()
  |> State.leaderP(server.selfP)
  |> Debug.state("New leader (#{server.server_num})'s state:", 1002)
  |> State.init_next_index()
  |> restart_append_entries_timers_for_followers()
  |> send_heartbeats_to_all()
end # establish_leadership

defp send_heartbeats_to_all(server) do
  Enum.each(server.servers, fn s ->
    unless s == server.selfP do
      send s, {:APPEND_ENTRIES_REQUEST, %{leader_term: server.curr_term, commit_index: 0, prev_term: 0, prev_index: 0, leader_entries: %{}, leader_pid: server.selfP}}
      # send s, {:APPEND_ENTRIES_REQUEST, %{leader_pid: server.selfP, leader_num: server.server_num, leader_term: server.curr_term, commit_index: nil}}
    end
  end)

  server
end # send_vote_requests_to_all

defp restart_append_entries_timers_for_followers(server) do
  Enum.reduce(server.servers, server, fn follower_pid, acc ->
    if follower_pid != acc.selfP do
      Timer.restart_append_entries_timer(acc, follower_pid)
    else
      acc
    end
  end) # Enum.reduce
end # restart_append_entries_timers_for_followers

# end

end # Vote
