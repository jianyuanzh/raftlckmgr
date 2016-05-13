# raftlckmgr

## Application, Client, Session and TCP connection

The client manages a TCP connection to the ensemble leader. To operate on locks, the
application needs to create sessions. All session multiplex the underlying TCP connection.

The session is blocked and non-thread-safe. If multiple threads in the application wants
to operate on different locks simultanously, they should create multiple sessions.
