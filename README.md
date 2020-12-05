# tap-google-play

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from Google Play store through [google-play-api](https://github.com/facundoolano/google-play-api)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

