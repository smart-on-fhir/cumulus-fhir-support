# Cumulus FHIR Support

This library holds FHIR support code for the Cumulus project as a whole.

## Installing

```shell
pip install cumulus-fhir-support
```

## Examples

### pyarrow_schema_from_rows

```python3
import cumulus_fhir_support

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
                }
            }],
        }]
    },
]

# The resulting schema will be both wide (every toplevel column)
# and deep enough for every field in `rows`.
# That is, both the non-present toplevel field "telecom" and the deeper
# field "extension.extension.valueCoding.system" will be in the schema.
schema = cumulus_fhir_support.pyarrow_schema_from_rows("Patient", rows)
```
