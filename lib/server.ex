
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

  # { :APPEND_ENTRIES_REQUEST, ...

  # { :APPEND_ENTRIES_REPLY, ...

  # { :APPEND_ENTRIES_TIMEOUT, ...

  { :VOTE_REQUEST, %{term: term, candidate_id: candidate_id}} ->
    server
    |> Vote.handle_vote_request(term, candidate_id)


  { :VOTE_REPLY, %{term: term, voter: voter, vote_granted: vote_granted}} ->
    server
    |> Vote.handle_vote_reply(term, voter, vote_granted)

  { :ELECTION_TIMEOUT, %{term: term, election: _election} } ->

    if server.role == :FOLLOWER || server.role == :CANDIDATE do
        server
        |> Timer.restart_election_timer()
        |> State.inc_election()
        |> State.role(:CANDIDATE)
        |> State.voted_for(server.server_num)
        |> State.add_to_voted_by(server.server_num)
        |> Debug.state("Server state:")
        |> send_vote_requests_to_all()
        |> next()
    end

  # { :CLIENT_REQUEST, ...

   unexpected ->
      Helper.node_halt("***** Server: unexpected message #{inspect unexpected}")

  end # receive

  server
  # |> State.cancel_election_timer()
  |> next()

end # next

def send_vote_requests_to_all(server) do
  for server <- server.servers do
    send server, {:VOTE_REQUEST, %{term: server.curr_term, candidate_id: server.server_num}}
  end
end


end # Server
