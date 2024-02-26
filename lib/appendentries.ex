
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2
defmodule AppendEntries do

    def receive_apes_req(server, %{prev_term: prev_term, prev_index: prev_index, leader_term: leader_term, leader_entries: leader_entries, commit_index: commit_index}) do
      server =
        if server.curr_term < leader_term && server.role != :FOLLOWER do
          server
          |> Vote.stepdown(%{term: leader_term})
        else
          server
        end

      if leader_term < server.curr_term do send server.leaderP, {:APPEND_ENTRIES_REPLY, server.selfP, server.curr_term, false, nil} end

      granted = ((prev_index <= Log.last_index(server) && prev_term == Log.term_at(server, prev_index)) || prev_index == 0) && (leader_term == server.curr_term)

      server =
        if granted do
            server
            |> store_entries(%{entries: leader_entries, prev_index: prev_index, commit_index: commit_index})
        else
          server
        end

      if server.curr_term == leader_term do send server.leaderP, {:APPEND_ENTRIES_REPLY, server.selfP, server.curr_term, granted, server.commit_index} end
      server
    end

    def send_apes_to_leader_followers(server, %{follower_pid: follower_pid}) do
      if server.next_index[follower_pid] < (Log.last_index(server) + 1) do
        follower_prev_index = server.next_index[follower_pid] - 1
        apes = Log.get_entries(server, (follower_prev_index + 1)..Log.last_index(server))
        prev_term = Log.term_at(server, follower_prev_index)

        server
        |> Timer.restart_append_entries_timer(follower_pid)

        send follower_pid, {:APPEND_ENTRIES_REQUEST, server.curr_term, follower_prev_index, prev_term, apes, server.commit_index}
        server
      else
        server
        |> Timer.restart_append_entries_timer(follower_pid)

        send follower_pid, {:APPEND_ENTRIES_REQUEST, server.curr_term, server.commit_index}
        server
      end
    end

    def receive_apes_reply(server, %{follower_term: _follower_term, granted: granted, follower_pid: follower_pid, follower_last_index: follower_last_index}) do
      server =
        if granted do
          State.next_index(server, follower_pid, follower_last_index + 1)
        else
          State.next_index(server, follower_pid, max(server.next_index[follower_pid] - 1, 1))
          |> send_apes_to_leader_followers(%{follower_pid: follower_pid})
        end

      counter =
        for i <- (server.last_applied + 1)..Log.last_index(server) // 1,
          into: %{},
          do: {i, count_followers(server, i, server.selfP)}

      Enum.each(counter, fn {index, count} ->
        if count >= server.majority do
          send(server.databaseP, {:DB_REQUEST, Log.request_at(server, index), index})
        else
          :ok
        end
      end)

      server
    end

    defp count_followers(server, index, self_pid) do
      Enum.reduce(server.servers, 1, fn follower_pid, count ->
        if follower_pid != self_pid && server.next_index[follower_pid] > index do
          count + 1
        else
          count
        end
      end)
    end


    def store_entries(server, %{entries: entries, prev_index: _prev_index, commit_index: _commit_index}) do
      divergence_points =
        for {index, v} <- entries do
          if Log.last_index(server) >= index do
            if server.log[index].term != v.term do index else nil end
          else
            index
          end
        end

      divergence_index = Enum.min(divergence_points)

      entries =
        if divergence_index != nil do
          Map.drop(entries, Enum.to_list(0..(divergence_index - 1)))
        else
          %{}
        end

      server =
        if divergence_index != nil && divergence_index < Log.last_index(server) do
          Log.delete_entries_from(server, divergence_index)
        else
          server
        end

      server =
        if entries != %{} do
          server
          |> Log.merge_entries(entries)
          |> State.commit_index(Log.last_index(server))
        else
          server
        end

      server
    end

  end # AppendEntriess
