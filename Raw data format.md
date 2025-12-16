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

Biometric TypeTags
TypeTag 	Description
EA 	EDA- Electrodermal Activity
EL 	EDL- Electrodermal Level
ER 	EDR- Electrodermal Response (EmotiBit V4+ combines ER into EA signal)
PI 	PPG Infrared
PR 	PPG Red
PG 	PPG Green
T0 	Temperature (only on EmotiBit Alpha/Beta V1, V2, V3)
T1 	Temperature
TH 	Temperature via Medical-grade Thermopile (only on EmotiBit MD)
AX 	Accelerometer X
AY 	Accelerometer Y
AZ 	Accelerometer Z
GX 	Gyroscope X
GY 	Gyroscope Y
GZ 	Gyroscope Z
MX 	Magnetometer X
MY 	Magnetometer Y
MZ 	Magnetometer Z
SA 	Skin Conductance Response (SCR) Amplitude
SR 	Skin Conductance Response (SCR) Rise Time
SF 	Skin Conductance Response (SCR) Frequency
HR 	Heart Rate
BI 	Heart Inter-beat Interval
H0 	Humidity (only on EmotiBit Alpha/Beta V1, V2, V3)