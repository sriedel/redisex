# RedisEx

A simple Redis connector for Elixir

Most commands have been implemented, except for Pub/Sub, MONITOR, DEBUG, and
the Transaction commands.

## Usage
```
iex(1)>   client = RedisEx.Client.connect( "127.0.0.1", 6379 )
RedisEx.Client.Client[socket: #Port<0.3393>]
iex(2)>   RedisEx.Client.set( client, "foo", "bar" )
{:ok, "OK"}
iex(3)>   RedisEx.Client.get( client, "foo" )
{:ok, "bar"}
```

## TODOs
- unit test client 
- Implement ALL the commands!
- Add an OTP Server for pub/sub operations
- Add an OTP Server for monitor 
