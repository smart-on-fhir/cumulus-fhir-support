# Cumulus FHIR Support

This library holds FHIR support code for the Cumulus project as a whole.

## Installing

```shell
pip install cumulus-fhir-support
```

## API

### list_multiline_json_in_dir

Lists available multiline JSON files in the target directory
(allowing filtering by FHIR resource).

Files with the `.jsonl` or `.ndjson` suffixes are supported.
Files with an additional `.gz` suffix will also be returned.

```python3
import cumulus_fhir_support as cfs

cfs.list_multiline_json_in_dir("/")
# {
#     "/con1.ndjson.gz": "Condition",
#     "/pat1.jsonl": "Patient",
#     "/random.jsonl": None,
# }

cfs.list_multiline_json_in_dir("/", "Patient")
# {
#     "/pat1.jsonl": "Patient",
# }

cfs.list_multiline_json_in_dir("/", ["Condition", "Patient"])
# {
#     "/con1.ndjson.gz": "Condition",
#     "/pat1.jsonl": "Patient",
# }

cfs.list_multiline_json_in_dir("/does-not-exist/")
# {}

cfs.list_multiline_json_in_dir("s3://mybucket/", fsspec_fs=s3_fs)
# {
#     "/mybucket/procs.ndjson": "Procedure",
# }
```

### read_multiline_json

Iterates over a single multiline JSON file.

Files with the `.gz` extension are automatically uncompressed.

```python3
import cumulus_fhir_support as cfs

list(cfs.read_multiline_json("/pat1.jsonl"))
# [
#     {"resourceType": "Patient", "id": "pat1", "birthDate": "2020-10-16"},
#     {"resourceType": "Patient", "id": "pat2", "birthDate": "2013-04-18"},
# ]

list(cfs.read_multiline_json("/does-not-exist.ndjson"))
# []

list(cfs.read_multiline_json("/mybucket/procs.ndjson", fsspec_fs=s3_fs))
# [
#     {"resourceType": "Procedure", "id": "proc1", "status": "stopped"},
# ]
```

### read_multiline_json_with_details

Lower level function to iterate over a single multiline JSON file, with extra line metadata.

Files with the `.gz` extension are automatically uncompressed.

```python3
import cumulus_fhir_support as cfs

list(cfs.read_multiline_json_with_details("/pat1.jsonl"))
# [
#     {
#         "json": {"resourceType": "Patient", "id": "pat1", "birthDate": "2020-10-16"},
#         "line_num": 0,
#         "byte_offset": 0,
#     },
#     {
#         "json": {"resourceType": "Patient", "id": "pat2", "birthDate": "2013-04-18"},
#         "line_num": 1,
#         "byte_offset": 69,
#     },
# ]

list(cfs.read_multiline_json_with_details("/pat1.jsonl"), offset=69)
# [
#     {
#         "json": {"resourceType": "Patient", "id": "pat2", "birthDate": "2013-04-18"},
#         "line_num": 0,
#         "byte_offset": 0,
#     },
# ]
```

### read_multiline_json_from_dir

Iterates over every JSON object in a directory
(allowing filtering by FHIR resource).

Files with the `.gz` extension are automatically uncompressed.

```python3
import cumulus_fhir_support as cfs

list(cfs.read_multiline_json_from_dir("/"))
# [
#     {"resourceType": "Condition", "id": "con1", "onsetDateTime": "2011-11-24"},
#     {"resourceType": "Patient", "id": "pat1", "birthDate": "2020-10-16"},
#     {"resourceType": "Patient", "id": "pat2", "birthDate": "2013-04-18"},
#     {"description": "not a fhir object"},
# ]

list(cfs.read_multiline_json_from_dir("/", "Condition"))
# [
#     {"resourceType": "Condition", "id": "con1", "onsetDateTime": "2011-11-24"},
# ]

list(cfs.read_multiline_json_from_dir("/", ["Condition", "Patient"]))
# [
#     {"resourceType": "Condition", "id": "con1", "onsetDateTime": "2011-11-24"},
#     {"resourceType": "Patient", "id": "pat1", "birthDate": "2020-10-16"},
#     {"resourceType": "Patient", "id": "pat2", "birthDate": "2013-04-18"},
# ]

list(cfs.read_multiline_json_from_dir("/does-not-exist/"))
# []

list(cfs.read_multiline_json_from_dir("/mybucket/", fsspec_fs=s3_fs))
# [
#     {"resourceType": "Procedure", "id": "proc1", "status": "stopped"},
# ]
```

### pyarrow_schema_from_rows

Calculates a schema that can cover a given collection of FHIR objects.

```python3
import cumulus_fhir_support as cfs

rows = [
    {
        "resourceType": "Patient",
        "id": "1",
        "extension": [{
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
            "extension": [{
                "url": "ombCategory",
                "valueCoding": {
                    "code": "2135-2",
                    "display": "Hispanic or Latino",
                    "system": "urn:oid:2.16.840.1.113883.6.238",
                },
            }],
        }],
    },
]

# The resulting schema will be both wide (every toplevel column)
# and deep enough for every field in `rows`.
# That is, both the non-present toplevel field "telecom" and the deeper
# field "extension.extension.valueCoding.system" will be in the schema.
schema = cfs.pyarrow_schema_from_rows("Patient", rows)
```

### FhirClient

Connect to a FHIR server with a variety of authentication options and retries built-in.

```python3
import cumulus_fhir_support as cfs

client = cfs.FhirClient("https://r4.smarthealthit.org", {"Patient"})

async with client:
    response = await client.request("GET", "Patient/2cda5aad-e409-4070-9a15-e1c35c46ed5a")
    print(response.json())
```
