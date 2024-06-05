Service is implemented using Python and Quart framework, which is quite similar to Flask with the addition of asyncrnous support.
For the DB i chsoe Mongo that resides on Atlas Cloud.

The service is available at the following url: 
https://wdata-task.onrender.com/

API call examples:

https://wdata-task.onrender.com/weather/insight?lon=51.5&lat=24.5&condition=rainyAndCold

https://wdata-task.onrender.com/load_to_db

https://wdata-task.onrender.com/clear_db

Possible Pitfalls/Optimizations:
1. Improve RAM Utilization
   - Background:
      I broke down the CSV files into chunks of data to avoid RAM exhaustion when reading a big file.
      However, when I implemented asynchronous tasks, I faced a phenomenon in which Python still preserved many references to the chunks of data while waiting for Mongo to finish writing.
      Therefore Python's garbage collector did not evict the data from RAM as I expected, which actually caused the worker on Render.com to terminate due to RAM exhaustion.
      To solve this problem I used a semaphore, which restricts only 4 asynchronous tasks to run simultaneously.
   - Possible optimization: Tweak BUFFER_SIZE and the number of semaphores to utilize the RAM on the machine better.
2. Error Handling - It is better to catch more specific errors rather than catching 'Exception'.
3. Indexing - My decision to use a composite index on longitude and latitude suits the task's required queries, but we might consider different indexing to support more queries.
