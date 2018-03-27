## CleverTap golang csv upload tool

Installation:
```
go get github.com/ankit-arora/clevertap-csv-upload 
go install github.com/ankit-arora/clevertap-csv-upload
```

Arguments:
```
  -csv string               Absolute path to the csv file
  
  -id string                CleverTap Account ID
  
  -p string                 CleverTap Account Passcode
  
  -t string                 The type of data, either profile or event, defaults to profile (default "profile")
  
  -evtName string           Event name. Required only when uploading events. Each CSV file can only have one type of event
  
  -r string                 The account region, either eu or in, defaults to eu (default "eu")
  
  -dryrun                   Do a dry run, process records but do not upload
  
```

Example Events upload:
```
clevertap-csv-upload -csv="/Users/ankit/Documents/in.csv" -id="XXX-XXX-XXXX" -p="XXX-XXX-XXXX" -t="event" -evtName="Product Viewed"

```

Example Profiles upload:
```
clevertap-csv-upload -csv="/Users/ankit/Documents/in.csv" -id="XXX-XXX-XXXX" -p="XXX-XXX-XXXX"
```

NOTE:  you must include one of identity, objectID, FBID or GPID, in your data.  Email addresses can serve as an identity value, but the key must be identity.
