
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Vote do
  def handle_vote_request(server, %{term: term, candidate_id: candidate_id, last_log_index: last_log_index, last_log_term: last_log_term}) do
    # Case 1: If request term is less than server's current term, reject vote
    case server.role do
      :LEADER ->
        # case
        # If server is leader, reject vote
        # TODO: consider network partitioning
        # if term < server.curr_term do

        # elsif term > server.curr_term do

        # else
        # end
        vote_granted = false
        {vote_granted, server}
      :FOLLOWER ->
        vote_granted = true
        cond do
          term < server.curr_term ->
            vote_granted = false
          term > server.curr_term ->
            # TODO: logging
            if server.voted_for in [nil, candidate_id] do
              vote_granted = true
            else
              vote_granted = false
            end
          true ->
             server
        end

        server =
          if vote_granted do
            server
            |> State.voted_for(candidate_id)
          else
            server
          end

        reply = %{term: server.curr_term, vote_granted: vote_granted}
        if vote_granted do
          server
          |> Timer.restart_election_timer()
          |> State.curr_term(term)
        end

        send(Enum.at(server.servers, candidate_id), {:VOTE_REPLY, reply})


      :CANDIDATE ->
        vote_granted = true
        cond do
          term < server.curr_term ->
            vote_granted = false
          term > server.curr_term ->
            # TODO: logging
            if server.voted_for in [nil, candidate_id] do
              vote_granted = true
            else
              vote_granted = false
            end
          true ->
             server
        end
        server =
          if vote_granted do
            server
            |> State.voted_for(candidate_id)
          else
            server
          end

        if vote_granted do
          server
          |> State.curr_term(term)
          |> Timer.restart_election_timer()
        end

        reply = %{term: server.curr_term, voter: server.server_num, vote_granted: vote_granted}
        send Enum.at(server.servers, candidate_id), {:VOTE_REPLY, reply}
    end
  end # handle_vote_request

  def handle_vote_reply(server, %{term: term, voter: voter, vote_granted: vote_granted}) do
    case server.role do
      :LEADER -> server
      :FOLLOWER -> server
      :CANDIDATE ->
        cond do
          term > server.curr_term ->
            server
            |> State.role(:FOLLOWER)
            |> State.curr_term(term)

          true ->
            server =
              if vote_granted do
                server
                |> State.add_to_voted_by(server.server_num) #s,v
              else
                server
              end
            if State.vote_tally(server) >= State.majority(server) do
              server
              |> State.role(:LEADER)      # Server becomes leader
              # |> State.init_next_index()  # Initialise next_index, as new leader
              # |> State.init_match_index() # Initialise match_index, as new leader
            end
        end
    end


    # if term > server.curr_term do
    #   # If term in reply is greater than current term, convert to follower
    #   server
    #   |> State.role(:FOLLOWER)
    #   |> State.curr_term(term)
    # else
    #   if vote_granted do
    #     updated_server = State.add_to_voted_by(server, )
    #     if State.vote_tally(updated_server) >= State.majority(server) do
    #       # Won election
    #       updated_server
    #       |> State.role(:LEADER)      # Server becomes leader
    #       |> State.init_next_index()  # Initialise next_index, as new leader
    #       |> State.init_match_index() # Initialise match_index, as new leader
    #     else
    #       updated_server
    #     end
    #   else
    #     # Vote was not granted, so do nothing

    #   end

    # end
  end # handle_vote_reply

end # Vote
