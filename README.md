## CleverTap golang csv upload tool

Installation:
```
go get github.com/ankit-arora/clevertap-csv-upload 
```

Arguments:
```
  -csv string               Absolute path to the csv file
  
  -id string                CleverTap Account ID
  
  -p string                 CleverTap Account Passcode
  
  -t string                 The type of data, either profile or event, defaults to profile (default "profile")
  
  -evtName string           Event name. Required only when uploading events. Each CSV file can only have one type of event
  
  -dryrun                   Do a dry run, process records but do not upload
  
```

Example:
```
clevertap-csv-upload -csv="/Users/ankit/Documents/in.csv" -id="XXX-XXX-XXXX" -p="XXX-XXX-XXXX" -t="event" -evtName="Product Viewed"

```

NOTE:  you must include one of identity, objectID, FBID or GPID, in your data.  Email addresses can serve as an identity value, but the key must be identity.
