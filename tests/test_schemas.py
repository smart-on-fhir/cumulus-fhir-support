"""Tests for schemas.py"""

import unittest

import pyarrow

import cumulus_fhir_support as support


class SchemaDetectionTests(unittest.TestCase):
    """Test case for schema detection"""

    def test_makes_wide_schema(self):
        """Verify we write out a wide schema even when presented with nothing"""
        schema = support.pyarrow_schema_from_rows("Patient")
        self.assertListEqual(
            [
                "resourceType",
                "id",
                "implicitRules",
                "language",
                "meta",
                "contained",
                "extension",
                "modifierExtension",
                "text",
                "active",
                "address",
                "birthDate",
                "communication",
                "contact",
                "deceasedBoolean",
                "deceasedDateTime",
                "gender",
                "generalPractitioner",
                "identifier",
                "link",
                "managingOrganization",
                "maritalStatus",
                "multipleBirthBoolean",
                "multipleBirthInteger",
                "name",
                "photo",
                "telecom",
            ],
            schema.names,
        )

        # Spot check a few of the types
        self.assertEqual(pyarrow.string(), schema.field("id").type)
        self.assertEqual(pyarrow.bool_(), schema.field("deceasedBoolean").type)
        self.assertEqual(pyarrow.int32(), schema.field("multipleBirthInteger").type)
        # Note how struct types only have basic types inside of them - this is intentional,
        # no recursion of structs is done
        self.assertEqual(
            pyarrow.struct(
                {"id": pyarrow.string(), "div": pyarrow.string(), "status": pyarrow.string()}
            ),
            schema.field("text").type,
        )
        self.assertEqual(
            pyarrow.list_(pyarrow.struct({"id": pyarrow.string(), "preferred": pyarrow.bool_()})),
            schema.field("communication").type,
        )

    def test_detected_fields_are_included_and_expanded(self):
        """Verify that deep (detected) fields are also included, with Coding expansion"""
        # Make sure that we include different deep fields for each - final schema should be a union
        rows = [
            {"stage": [{"type": {"coding": [{"version": "1.0"}]}}]},
            {"onsetRange": {"low": {"value": 1.0}}},
        ]
        schema = support.pyarrow_schema_from_rows("Condition", rows)

        # Start with simple, non-present CodeableConcept at level zero.
        # This should be fully described.
        self.assertEqual(
            pyarrow.struct(
                {
                    "id": pyarrow.string(),
                    "coding": pyarrow.list_(
                        pyarrow.struct(
                            {
                                "id": pyarrow.string(),
                                "code": pyarrow.string(),
                                "display": pyarrow.string(),
                                "system": pyarrow.string(),
                                "userSelected": pyarrow.bool_(),
                                "version": pyarrow.string(),
                            }
                        )
                    ),
                    "text": pyarrow.string(),
                }
            ),
            schema.field("code").type,  # CodeableConcept type
        )
        # While a deeper non-present CodeableConcept should be ignored
        self.assertEqual(
            pyarrow.list_(
                pyarrow.struct(
                    {
                        "id": pyarrow.string(),
                        # "code" field is missing (CodeableConcept type)
                        # "detail" field is missing (Reference type)
                    }
                )
            ),
            schema.field("evidence").type,  # BackboneElement type
        )
        # But if any piece of a deep CodeableConcept is present, it gets fully expanded.
        self.assertEqual(
            pyarrow.list_(
                pyarrow.struct(
                    {
                        "id": pyarrow.string(),
                        # "assessment" field is missing (Reference type)
                        # "summary" field is missing (CodeableConcept type)
                        # But the "type" is here in full because a piece of it was in the input
                        "type": pyarrow.struct(
                            {
                                "id": pyarrow.string(),
                                "coding": pyarrow.list_(
                                    pyarrow.struct(
                                        {
                                            "id": pyarrow.string(),
                                            "code": pyarrow.string(),
                                            "display": pyarrow.string(),
                                            "system": pyarrow.string(),
                                            "userSelected": pyarrow.bool_(),
                                            "version": pyarrow.string(),
                                        }
                                    )
                                ),
                                "text": pyarrow.string(),
                            }
                        ),
                    }
                )
            ),
            schema.field("stage").type,  # BackboneElement type
        )
        # Other deep-and-partial elements do not get the same expansion treatment.
        # Here is a deep Quantity element.
        # The parts present in the input are also in the schema, but only those parts.
        self.assertEqual(
            pyarrow.struct(
                {
                    "id": pyarrow.string(),
                    "low": pyarrow.struct(
                        {
                            "value": pyarrow.float64(),
                        }
                    ),
                }
            ),
            schema.field("onsetRange").type,
        )

    def test_schema_types_are_coerced(self):
        """Verify that fields with "wrong" input types (like int instead of float) are corrected"""
        # Make sure that we include both wide and deep fields.
        # Both should be coerced into floats.
        rows = [
            {"quantityQuantity": {"value": 1}},
            {"quantityRange": {"low": {"value": 2}}},
        ]
        schema = support.pyarrow_schema_from_rows("ServiceRequest", rows)

        self.assertEqual(
            pyarrow.float64(), schema.field("quantityQuantity").type.field("value").type
        )
        self.assertEqual(
            pyarrow.float64(),
            schema.field("quantityRange").type.field("low").type.field("value").type,
        )

    def test_non_spec_field_are_ignored(self):
        """Verify that a field not in the FHIR spec is handled gracefully"""
        rows = [{"invalid_field": "nope"}]
        schema = support.pyarrow_schema_from_rows("Observation", rows)

        self.assertNotIn("invalid_field", schema.names)
