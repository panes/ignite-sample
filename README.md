# ignite-sample

Run MyIgniteLoadRunnerTest.run() to reproduce a TTL behavior about removing expired entries from the memory.

Context:

Create an ignite client (in client mode false) and put some data to it with very small expiration time and TTL enabled.

Each time the thread is running it'll remove all the entries that expired, but after few attempts this thread is not removing all the expired entries, some of them are staying in memory and are not removed by this thread execution.

That means we got some expired data in memory, and it's something we want to avoid.   