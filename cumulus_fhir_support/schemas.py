"""Detect FHIR resource schemas"""

from collections import namedtuple
from collections.abc import Iterable
from typing import Any

import pyarrow
from fhirclient.models import (
    codeableconcept,
    coding,
    element,
    extension,
    fhirabstractbase,
    fhirdate,
    fhirelementfactory,
    period,
    reference,
)

FhirProperty = namedtuple(
    "FhirProperty", ["name", "json_name", "pytype", "is_list", "of_many", "required"]
)

# We include one level of the FHIR spec in our schema, regardless of what's in the source data.
# This is to help downstream SQL by at least making sure each column is in the schema.
LEVEL_INCLUSION = 1


def pyarrow_schema_from_rows(
    resource_type: str, rows: Iterable[dict] | None = None
) -> pyarrow.Schema:
    """
    Creates a PyArrow schema based off the named resource (like 'Observation') and row contents.

    Note that this schema will not be deep (fully nested all the way down),
    it will simply be wide (covering each toplevel field, each likely nullable).
    But it *will* at least include every field contained in the batch.

    Non-FHIR-spec fields will not be present in the final schema.
    All fields will be marked nullable.

    Certain elements are guaranteed to be fully specified, if any part of them is specified:
    - CodeableConcept
    - Coding
    - Period
    For example, if `nested.period.start` is in the source data but `.end` is not, `.end` will
    still be in the returned schema.

    :param resource_type: the FHIR resource name to create a schema for
    :param rows: optionally a set of JSON FHIR resources to ensure are covered by the schema
    :returns: a PyArrow schema that covers the unified shape of all provided rows
    """
    rows = rows or []

    # Examine batch to see the full shape of it, in order to detect any deeply nested fields
    # that we want to make sure to include in the final schema (normally, we go wide but only as
    # deep as we need to)
    # Note: be careful to only iterate through `rows` once, to allow passing in pure iterables.
    batch_shape = {}
    contained_types = set()
    for row in rows:
        # Build up a complete picture of the shape of all rows
        batch_shape = _get_shape_of_dicts(batch_shape, row)

        # Also gather up which kind of contained resources exist.
        for contained_obj in row.get("contained", []):
            if contained_type := contained_obj.get("resourceType"):
                contained_types.add(contained_type)

    # Now actually create the schema
    schema = _create_pyarrow_schema_for_resource(resource_type, batch_shape)
    schema = _include_contained_schemas(schema, contained_types, batch_shape)
    return schema


def _get_shape_of_dicts(total_shape: dict | None, item: Any) -> dict:
    """
    Examines `item` and gives a description of its "shape".

    Shape here means a dictionary tree of fields, like {"id": {}, "code": {"text": {}}}
    where empty dictionaries indicate no further children.

    This is not a generic concept at all - it's purely to aid with creating a schema for a batch
    of input rows. This shape will tell us which FHIR fields to include in our schema.

    Example Input:
    {"address": [{"street": "123 Main St", "city": "Springfield"}], "name": "Jane Smith"}

    Example output:
    {"address": {"street": {}, "city": {}}, "name": {}}

    :param total_shape: a pre-existing shape that we will merge fields into
    :param item: the current item being examined
    :returns: a shape for this item and its descendants (will be same dict as total_shape)
    """
    total_shape = total_shape or {}

    if isinstance(item, list):
        for x in item:
            total_shape = _get_shape_of_dicts(total_shape, x)
    elif isinstance(item, dict):
        for key, val in item.items():
            total_shape[key] = _get_shape_of_dicts(total_shape.get(key), val)

    return total_shape


def _include_contained_schemas(
    schema: pyarrow.Schema, contained_types: set[str], batch_shape: dict
) -> pyarrow.Schema:
    """
    This will include all contained resource schemas into one big contained schema.

    Specifically, any field found in the shape of the "contained" field will be included,
    as long as any resource in the contained list (detected via "resourceType") has the field.

    Also see https://github.com/smart-on-fhir/cumulus-etl/issues/250 for discussion
    of whether it is wise to just comingle the schemas like this.
    """
    if not contained_types:
        return schema  # no need to do anything
    contained_shape = batch_shape.get("contained")

    # Allow any found fields in any of the contained types
    fields = {}
    for contained_type in sorted(contained_types):
        subschema = _create_pyarrow_schema_for_resource(contained_type, contained_shape, wide=False)
        for name in subschema.names:
            fields[name] = subschema.field(name)  # will overwrite previous field of same name
    fields = [fields[name] for name in sorted(fields)]  # sort for a consistent order

    contained_index = schema.get_field_index("contained")
    schema = schema.remove(contained_index)
    return schema.insert(
        contained_index,
        pyarrow.field("contained", pyarrow.list_(pyarrow.struct(fields))),
    )


def _create_pyarrow_schema_for_resource(
    resource_type: str, batch_shape: dict, wide: bool = True
) -> pyarrow.Schema:
    """
    Creates a PyArrow schema based off the named resource (like 'Observation').

    This schema will be as wide as the spec is and as deep as the batch_shape is.

    batch_shape is a dictionary tree of fields to include, like {"id": {}, "code": {"text": {}}}
    where empty dictionaries indicate no children (but the parent should still be included).
    """
    instance = fhirelementfactory.FHIRElementFactory.instantiate(resource_type, None)

    # fhirclient doesn't include `resourceType` in the list of properties, because it's only
    # used in ndjson representations. But it's useful to have, so add it manually.
    type_field = pyarrow.field("resourceType", pyarrow.string())

    level = 0 if wide else 2
    return pyarrow.schema(
        [type_field, *_fhir_obj_to_pyarrow_fields(instance, batch_shape, level=level)]
    )


def _fhir_obj_to_pyarrow_fields(
    base_obj: fhirabstractbase.FHIRAbstractBase, batch_shape: dict, *, level: int
) -> list[pyarrow.Field]:
    """Convert a FHIR instance to a PyArrow Field schema list"""
    fhir_properties = map(FhirProperty._make, base_obj.elementProperties())
    pa_properties = []

    for fhir_property in fhir_properties:
        if pa_property := _fhir_to_pyarrow_property(
            fhir_property,
            base_obj=base_obj,
            batch_shape=batch_shape,
            level=level,
        ):
            pa_properties.append(pa_property)
        if pa_sunder := _sunder_to_pyarrow_property(fhir_property, batch_shape=batch_shape):
            pa_properties.append(pa_sunder)

    return pa_properties


def _fhir_to_pyarrow_property(
    prop: FhirProperty,
    *,
    base_obj: fhirabstractbase.FHIRAbstractBase | None = None,
    batch_shape: dict | None = None,
    level: int,
) -> pyarrow.Field | None:
    """Converts a single FhirProperty to a PyArrow Field, or None if this field should be skipped"""
    if batch_shape is not None:
        batch_shape = batch_shape.get(prop.json_name)

    # If we see a piece of an important/common element we like to grab the full schema for it.
    # This helps downstream SQL avoid dealing with incomplete objects.
    full_schema_types = (
        codeableconcept.CodeableConcept,
        coding.Coding,
        period.Period,
        reference.Reference,  # note: this won't include children of the `identifier` struct child
    )
    is_inside_full_schema_type = isinstance(base_obj, full_schema_types)
    is_extension_type = issubclass(prop.pytype, extension.Extension)
    force_inclusion = is_inside_full_schema_type and not is_extension_type

    # OK how do we handle this field? Include or exclude - descend or not?
    present_in_shape = batch_shape is not None
    include_in_schema = present_in_shape or force_inclusion
    is_struct = issubclass(prop.pytype, fhirabstractbase.FHIRAbstractBase)

    if is_struct:
        if level >= LEVEL_INCLUSION and not include_in_schema:
            # Skip this element entirely and do not descend, to avoid infinite recursion.
            # Note that in theory this might leave a struct with no child fields
            # (if a struct's only children were also structs),
            # which parquet/spark would have an issue with because they won't allow empty structs.
            # But in practice with FHIR, all BackboneElements have at least an id (string) field,
            # so we dodge that bullet.
            return None
        # Recurse!
        children = _fhir_obj_to_pyarrow_fields(prop.pytype(), batch_shape, level=level + 1)
        if not children:
            # Downstream consumers may rightfully be confused by a struct with no children.
            # For example, DuckDB raises an exception if it sees them.
            return None
        pyarrow_type = pyarrow.struct(children)
    else:
        if level > LEVEL_INCLUSION and not include_in_schema:
            # If we're deeper than our inclusion level,
            # bail if we don't actually see the field in the data
            return None
        pyarrow_type = _basic_fhir_to_pyarrow_type(prop.pytype)

    # Wrap lists in an ListType
    if prop.is_list:
        pyarrow_type = pyarrow.list_(pyarrow_type)

    # Mark all types as nullable, don't worry about the prop.required field.
    # We don't need to be in the business of validation, we just want to provide a schema.
    return pyarrow.field(prop.json_name, pyarrow_type, nullable=True)


def _sunder_to_pyarrow_property(
    prop: FhirProperty,
    *,
    batch_shape: dict | None = None,
) -> pyarrow.Field | None:
    """
    Checks for a FhirProperty's "sunder" sibling and returns a PyArrow field for it.

    A sunder (single underscore) field is an adjacent JSON field for primitive types that don't
    otherwise have a place to put extension information. So "status" might have a sibling
    "_status" field.

    See http://hl7.org/fhir/R4/json.html#primitive for more information.

    Returns None if the sunder field isn't present.
    """
    # First, check if the sunder version is even present.
    if not batch_shape or f"_{prop.json_name}" not in batch_shape:
        return None

    # Make a fake property definition and see if it's good.
    sunder_prop = FhirProperty(
        name=f"_{prop.name}",
        json_name=f"_{prop.json_name}",
        pytype=element.Element,
        is_list=prop.is_list,
        of_many=prop.of_many,
        required=prop.required,
    )
    return _fhir_to_pyarrow_property(sunder_prop, level=LEVEL_INCLUSION, batch_shape=batch_shape)


def _basic_fhir_to_pyarrow_type(pytype: type) -> pyarrow.DataType:
    """Converts a basic python type to a Pyspark type"""
    if pytype is int:
        return pyarrow.int32()
    elif pytype is float:
        # TODO: the FHIR spec suggests that float64 might not even be enough:
        #  From https://www.hl7.org/fhir/R4/datatypes.html:
        #  "In object code, implementations that might meet this constraint are GMP implementations
        #   or equivalents to Java BigDecimal that implement arbitrary precision, or a combination
        #   of a (64 bit) floating point value with a precision field"
        #  But for now, we are matching the inferred types from before we used a pre-calculated
        #  schema. We can presumably up-scale this at some point if we find limitations.
        return pyarrow.float64()
    elif pytype is str:
        return pyarrow.string()
    elif pytype is bool:
        return pyarrow.bool_()
    elif issubclass(pytype, fhirdate.FHIRDate):
        return pyarrow.string()  # just leave it as a string, like it appears in the JSON
    raise ValueError(f"Unexpected type: {pytype}")
