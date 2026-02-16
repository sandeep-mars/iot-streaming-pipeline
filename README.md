Situation: The company’s data dashboards were slow and took hours to update, meaning they couldn't see sensor alerts until it was too late.

Task: My goal was to build a "fast lane" for data that handles thousands of messages at once.

Action: I used PySpark and Kafka to create a "Structured Streaming" pipeline that cleans and organizes data as it arrives.

Result: I cut the waiting time from hours down to less than 60 seconds, allowing for immediate alerts.


Tech Stack

Compute: Spark (PySpark) 

Streaming: Kafka 

Storage: Delta Lake (for organized, safe data) 


Project Structure

src/: Contains the streaming_job.py (The main "engine" of the project).

tests/: Where I keep scripts to make sure the code works perfectly.
