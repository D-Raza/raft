
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule ClientRequest do

  def handle_client_request(server, req = %{cmd: _cmd, clientP: clientP, cid: cid}) do
    Debug.message(server, "+state", "Handle client request from #{inspect(cid)}", 1002)
    case server.role do
      :LEADER ->
        Debug.message(server, "+state", "Client request is handled by the Leader", 998)
        case determine_request_status(server, cid) do
          {:NEW_REQUEST, _} ->
            new_entry = %{term: server.curr_term, request: req}
              server
              |> Log.append_entry(new_entry)
              |> AppendEntries.send_entries_to_all_but_self()
          {:COMMITTED, old_entry} ->
            send old_entry.request.clientP, { :CLIENT_REPLY, %{ cid: old_entry.request.cid, leaderP: server.selfP}}
            server
          {:LOGGED_NOT_COMMITTED, _} -> server
        end
      _ ->
        Debug.message(server, "+state", "Client request is NOT handled by the Leader", 998)
        if server.leaderP != nil, do: send(clientP, { :CLIENT_REPLY, %{ reply: :NOT_LEADER, leaderP: server.leaderP, cid: cid}}) # not leader
        server
    end # case server.role
  end # handle_client_request

  defp determine_request_status(server, cid) do
    case Enum.find(server.log, fn {_, entry} -> entry.request.cid == cid end) do
      nil                                              -> {:NEW_REQUEST, nil}
      {index, entry} when index < server.commit_index  -> {:COMMITTED, entry}  # LOGS ARE 1-INDEXED
      {_, entry}                                       -> {:LOGGED_NOT_COMMITTED, entry}
    end
  end # determine_request_status

end # ClientRequest
