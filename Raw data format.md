### Raw data format

The raw data is stored in the following format. The following steps discuss using the EmotiBit DataParser to parse raw data files.

`EMOTIBIT_TIMESTAMP`,`PACKET#`,`NUM_DATAPOINTS`,`TYPETAG`,`VERSION`,`RELIABILITY`,`PAYLOAD`

-   **EMOTIBIT_TIMESTAMP:** milliseconds since EmotiBit bootup
-   **PACKET#:** sequentially increasing packet count
-   **NUM_DATAPOINTS:** Number of data points in the payload
-   **TYPETAG:** [type of data](https://github.com/EmotiBit/EmotiBit_Docs/blob/master/Working_with_emotibit_data.md#motibit-data-types) being sent
-   **VERSION:** packet protocol version
-   **RELIABILITY:** data reliability score out of 100 (for future use)
-   **PAYLOAD:** data points