
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Vote do
  # Case 1: If request term is less than server's current term, reject vote
  def handle_vote_request(server, %{term: term, candidate_pid: candidate_pid, candidate_num: candidate_num}) do
    case server.role do
      :LEADER ->
        # TODO: consider network partitioning
        IO.puts("Leader case being hit in handle_vote_request, leader is #{server.server_num}")
        cond do
          term < server.curr_term ->
            server
            |> Debug.message("+state", "This case should not be hit", 998)
          term > server.curr_term ->
            server
            |> Debug.message("+state", "This case should not be hit", 998)
          true ->
            server
            |> Debug.message("+state", "This case can be hit", 998)
        end
      :FOLLOWER ->
        cond do
          term < server.curr_term ->
            send(candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: false}})
            server
          term > server.curr_term ->
            # TODO: logging
            # if server.voted_for in [nil, candidate_num] do
            #   IO.inspect("server_num: #{server.server_num} server.voted_for #{server.voted_for} candidate_num: #{candidate_num}")
            #   true
            # else
            #   false
            # end
            vote_granted = server.voted_for in [nil, candidate_num]
            updated_server =
              if vote_granted do
                server
                |> State.voted_for(candidate_num)
                |> State.curr_term(term)
                |> Timer.restart_election_timer()
              else
                server
              end
            send(candidate_pid, {:VOTE_REPLY, %{term: updated_server.curr_term, voter: updated_server.server_num, vote_granted: vote_granted}})
            updated_server
          true ->
            send(candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: false}})
            server
        end
      :CANDIDATE ->
        cond do
          term < server.curr_term ->
            send(candidate_pid, {:VOTE_REPLY, %{term: server.curr_term, voter: server.server_num, vote_granted: false}})
            server
          # TODO: logging
          term > server.curr_term ->
            server
            |> stepdown(%{term: term})
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
          term > server.curr_term ->
            server
            |> stepdown(%{term: term})
            |> Debug.assert(term == server.current_term, "Assert")
          term < server.curr_term ->
            server
            |> Debug.message("+state", "This case should never hit", 998)
          true ->
            updated_server =
              if vote_granted do
                server
                |> State.add_to_voted_by(voter)
                |> Debug.message("+state", "Check", 1002)
              else
                server
              end
            updated_server =
              if State.vote_tally(updated_server) >= server.majority do
                updated_server
                |> State.role(:LEADER)      # Server becomes leader
                |> State.leaderP(server.selfP)
                |> Debug.message("+state", "Leader is now #{server.server_num}", 998)
                |> Timer.cancel_election_timer()
                |> State.init_next_index()
                |> send_heartbeats_to_all()
                # Set leaderP to server's canidate pid
                # |> State.init_next_index()  # Initialise next_index, as new leader
                # |> State.init_match_index() # Initialise match_index, as new leader
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
  |> State.new_voted_by()
  |> Timer.cancel_all_append_entries_timers()
  |> Timer.restart_election_timer()
end

def send_heartbeats_to_all(server) do
  Enum.each(server.servers, fn s ->
    unless s == server.selfP do
      send s, {:APPEND_ENTRIES_REQUEST, %{leader_pid: server.selfP, leader_term: server.curr_term, commit_index: nil}}
    end
  end)

  server
end # send_vote_requests_to_all






# end

end # Vote
