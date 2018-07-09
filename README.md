# Scalable Syslog

[![CI Badge][ci-badge]][ci-pipeline]

The scalable syslog is a consumer of [Loggregator][loggregator] which
transports application logs to syslog drains. It writes messages according to
[RFC 5424][rfc5424] and [RFC 6587][rfc6587]. Here is an example of how it
writes messages:

```
    + Protocol Version
    |
    |  + Timestamp                   + org_name.space_name.app_name                                       + ProcessID (Source)
    |  |                             |                                                                    |
    v  v                             v                                                                    v
<14>1 2017-10-05T15:00:55.432180389Z cf-lamb.development.dripspinner 3e0b1150-14b8-4a20-a1c5-d9e296f198ae [APP/PROC/WEB] - - msg 1 LogSpinner Log Message
 ^                                                                   ^                                                       ^
 |                                                                   |                                                       |
 + Priority                                                          + AppID                                                 + Log Message
```

If you see a decimal prefix followed by a space in your message, this is the
length prefixed to the message. This is used to frame syslog messages when
transmitting over a streaming protocol.

[loggregator]: https://github.com/cloudfoundry/loggregator
[ci-badge]:                 https://loggregator.ci.cf-app.com/api/v1/teams/main/pipelines/cf-syslog-drain/jobs/cf-syslog-drain-tests/badge
[ci-pipeline]:              https://loggregator.ci.cf-app.com/teams/main/pipelines/cf-syslog-drain
[rfc5424]:     https://tools.ietf.org/html/rfc5424
[rfc6587]:     https://tools.ietf.org/html/rfc6587
