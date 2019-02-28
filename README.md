# Script to extract data from Amplitude into local system based on period.

## Parameters

- start (Format: *YYYYMMDD*T*HH*)
- end (Format: *YYYYMMDD*T*HH*)

## Headers

- Authorization - Basic authorization followed by your credentials in base64 format.

For more informations, refer [here](https://amplitude.zendesk.com/hc/en-us/articles/205406637-Export-API-Export-Your-Project-s-Event-Data)

## Connect to local Mongo database in Docker

- Run `docker container run -d -p 27017:27017 mongo:xenial`
