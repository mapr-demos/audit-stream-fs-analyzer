# Audit stream FS analyzer
Demo for monitoring audit events for detecting files that have not been accessed in a long time.
## Quick start
1. Enable audit in system.
2. Create folder for storing table with files and volumes or they will be created on the root of MapRFS.
3. Start application.
    ```bash
    ./gradlew bootRun
    ```
    You can specify such parameters:
    * Server host DATABASE_HOST (default `node01`)
    * Path to table's folder DATABASE_NAME (default `/`)
    * Username DATABASE_USERNAME (default `mapr`)
    * Password DATABASE_PASSWORD (default `mapr`)
    ```bash
    DATABASE_HOST=YourHost DATABASE_NAME=YourPathToFolder DATABASE_USERNAME=YourUsername DATABASE_PASSWORD=YourPassword ./gradlew bootRun
    ```