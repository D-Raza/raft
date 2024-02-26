
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus,  v2

defmodule Database do

# _________________________________________________________ Database setters()
def seqnum(database, v)      do Map.put(database, :seqnum, v) end
def balances(database, k, v) do Map.put(database, :balances, Map.put(database.balances, k, v)) end

# _________________________________________________________ Database.start()
def start(config, db_num) do
  receive do
  { :BIND, serverP } ->
    database = %{                          # initialise database state variables
      config:   config,
      db_num:   db_num,
      serverP:  serverP,
      seqnum:   0,
      balances: Map.new,
    }
    database |> Database.next()
  end # receive
end # start

# _________________________________________________________ Database.next()
def next(database) do
  receive do
  { :DB_REQUEST, client_request, log_index } when log_index == database.seqnum + 1 ->
    { :MOVE, amount, account1, account2 } = client_request.cmd

    database = database |> Database.seqnum(database.seqnum+1)

    balance1 = Map.get(database.balances, account1, 0)
    balance2 = Map.get(database.balances, account2, 0)

    database
    |> Database.balances(account1, balance1 + amount)
    |> Database.balances(account2, balance2 - amount)
    |> Monitor.send_msg({ :DB_MOVE, database.db_num, database.seqnum, client_request.cmd })
    |> Database.send_reply_to_server(:OK, database.seqnum, client_request)
    |> Database.next()

  { :DB_REQUEST, _client_request, _log_index } ->
    database
    |> Database.next()

  unexpected ->
    Helper.node_halt(" *********** Database: unexpected message #{inspect unexpected}")
  end # receive
end # next

def send_reply_to_server(d, db_result, seqnum,client_request) do
  send d.serverP, { :DB_REPLY, db_result, seqnum, client_request}
  d
end # reply_to_server

end # Database
