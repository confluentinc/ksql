---
layout: page
title: Get the Validity of a Property
tagline: is_valid_property endpoint
description: The `/is_valid_property` resource tells you whether a property is prohibited from setting.
keywords: ksqldb, server, is_valid_property
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-rest-api/is_valid_property-endpoint.html';
</script>

The `/is_valid_property` resource tells you whether a property is prohibited from setting. You
can use the `curl` command to query the `/is_valid_property` endpoint:

```bash
curl -sX GET "http://localhost:8088/is_valid_property/propertyName" | jq '.'
```

If the property is not prohibited from setting, the endpoint should return `true`. Otherwise, the
output should resemble:

```json
{
  "@type": "generic_error",
  "error_code": 40000,
  "message": "One or more properties overrides set locally are prohibited by the KSQL server (use UNSET to reset their default value): [ksql.service.id]"
}
```