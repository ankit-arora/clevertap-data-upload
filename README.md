## CleverTap golang data upload tool

Installation:
```
go get github.com/CleverTap/clevertap-data-upload 
go install github.com/CleverTap/clevertap-data-upload
```

Arguments:
```
  -csv string               Absolute path to the csv file
  
  -id string                CleverTap Account ID
  
  -p string                 CleverTap Account Passcode
  
  -t string                 The type of data, either profile or event, defaults to profile (default "profile")
  
  -evtName string           Event name. Required only when uploading events. Each CSV file can only have one type of event
  
  -r string                 The account region, either eu, in, sk, or sg, defaults to eu (default "eu")
  
  -dryrun                   Do a dry run, process records but do not upload

  -mixpanelSecret           Mixpanel API secret key

  -startDate                Start date for exporting events from Mixpanel <yyyy-mm--dd>

  -endDate                  End date for exporting events <yyyy-mm-dd>

  -startTs                  Start timestamp for events upload in epoch
  
```

Example Events upload from CSV:
```
clevertap-data-upload -csv="/Users/ankit/Documents/in.csv" -id="XXX-XXX-XXXX" -p="XXX-XXX-XXXX" -t="event" -evtName="Product Viewed"

```

Example Profiles upload from CSV:
```
clevertap-data-upload -csv="/Users/ankit/Documents/in.csv" -id="XXX-XXX-XXXX" -p="XXX-XXX-XXXX"
```

NOTE: For CSV uploads, you must include one of identity, objectId, FBID or GPID, in your data.  Email addresses can serve as an identity value, but the key must be identity.

Example Events upload from Mixpanel:
```
clevertap-data-upload -id="XXX-XXX-XXXX" -p="XXX-XXX-XXXX" -mixpanelSecret="<mp api secret>" -t="event" -startDate="<yyyy-mm-dd>" -endDate="<yyyy-mm-dd>"

```

Example Profiles upload from Mixpanel:
```
clevertap-data-upload -id="XXX-XXX-XXXX" -p="XXX-XXX-XXXX" -mixpanelSecret="<mixpanel secret key>"

```