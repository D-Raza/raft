
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule ClientRequest do

# -- omitted

  def handle_client_request(server, %{cmd: cmd, clientP: clientP, cid: cid}) do
    case server.role do
      :LEADER ->
        case determine_request_processing_state(server, cid) do
          :DB_APPLIED_REQUEST ->
            send clientP, { :CLIENT_REPLY, %{ leaderP: server.leaderP, cid: cid}}
            server
          :UNPROCESSED_REQUEST ->
            server
            |> Log.append_entry(%{request: %{cmd: cmd, clientP: clientP, cid: cid}, term: server.curr_term})
            |> State.commit_index(Log.last_index(server))
            |> send_entries_to_all_but_self()
          :LOGGED_NOT_APPLIED_REQUEST -> server
        end

      _ -> send clientP, { :CLIENT_REPLY, %{ reply: :NOT_LEADER, leaderP: server.leaderP, cid: cid}} # not leader

    end
    server
  end # handle_client_request

  defp determine_request_processing_state(leader, cid) do
    # Determine if CID is in the applied range
    # print out the request

    IO.puts("entry bum : #{Log.entry_at(leader, leader.last_applied)}")

    IO.puts("request bum : #{Log.request_at(leader, leader.last_applied)}")

    applied_status = Enum.any?(1..leader.last_applied, fn i ->
      case Log.request_at(leader, i) do
        %{} = request when request.cid == cid -> true
        _ -> false
        # nil -> false
        # entry -> entry.cid == cid
      end
    end)
    # Determine if CID is in the comitted but not applied range
    committed_status = Enum.any?(leader.last_applied+1..leader.commit_index, fn i ->
      case Log.request_at(leader, i) do
        %{} = request when request.cid == cid -> true
        _ -> false
        # nil -> false
        # entry -> entry.cid == cid # entry.request?
      end
    end)

    cond do
      applied_status   -> :DB_APPLIED_REQUEST
      committed_status -> :LOGGED_NOT_APPLIED_REQUEST
      true             -> :UNPROCESSED_REQUEST
    end
  end # determine_request_processing_state

  defp send_entries_to_all_but_self(server) do
    Enum.each(server.servers, fn f_pid ->
      if f_pid != server.selfP, do: AppendEntries.send_entries(server, f_pid)
    end)
    server
  end # send_entries_to_all_but_self

end # ClientRequest
