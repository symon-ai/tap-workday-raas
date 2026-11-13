import json
import unittest
from unittest import mock

from singer import metadata
from singer.catalog import CatalogEntry, Schema
from singer.transform import SchemaMismatch

from tap_workday_raas import client, discover, sync
from tap_workday_raas.transform import WorkdayTransformer


xsd = """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:wd="urn:com.workday.report/Job_Profiles" elementFormDefault="qualified" attributeFormDefault="qualified" targetNamespace="urn:com.workday.report/Job_Profiles">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>
    <xsd:complexType name="Compensation_Eligibility_Rules_groupType">
        <xsd:sequence>
            <xsd:element name="DBS" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Bonus_Plan" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Target_Percent" type="xsd:decimal" minOccurs="0"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="Report_EntryType">
        <xsd:sequence>
            <xsd:element name="Job_Code" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Job_Profile" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Compensation_Eligibility_Rules_group" type="wd:Compensation_Eligibility_Rules_groupType" minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="Report_DataType">
        <xsd:sequence>
            <xsd:element name="Report_Entry" type="wd:Report_EntryType" minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

grouped_row = {
    "Job_Code": "JC100",
    "Job_Profile": "Commercial Banker",
    "Compensation_Eligibility_Rules_group": [
        {"DBS": "Y", "Bonus_Plan": "Commercial Bonus", "Target_Percent": "15"},
        {"DBS": "N", "Bonus_Plan": "Spot Bonus", "Target_Percent": "5.5"},
    ],
}


def _schema():
    return discover.generate_schema_for_report(xsd)


def _transform(record, schema=None):
    schema = schema or _schema()
    mdata = metadata.to_map(metadata.get_standard_metadata(schema, replication_method="FULL_TABLE"))
    with WorkdayTransformer() as transformer:
        return transformer.transform(record, schema, mdata)


class _StreamingResponse:
    status_code = 200

    def __init__(self, body):
        self._body = body

    def iter_content(self, chunk_size=512):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i:i + chunk_size]

    def raise_for_status(self):
        return None


class _GetContext:
    def __init__(self, response):
        self._response = response

    def __enter__(self):
        return self._response

    def __exit__(self, *args):
        return False


class GroupColumnSchemaTest(unittest.TestCase):
    def test_group_becomes_array_of_objects_with_sub_fields(self):
        group = _schema()["properties"]["Compensation_Eligibility_Rules_group"]

        self.assertEqual(group["type"], "array")
        self.assertEqual(group["items"]["type"], "object")
        self.assertEqual(
            group["items"]["properties"],
            {
                "DBS": {"type": ["string", "null"]},
                "Bonus_Plan": {"type": ["string", "null"]},
                "Target_Percent": {"type": ["number", "null"]},
            },
        )


class GroupColumnTransformTest(unittest.TestCase):
    def test_group_rows_keep_every_instance_and_sub_field(self):
        result = _transform(grouped_row)

        self.assertEqual(result["Job_Code"], "JC100")
        self.assertEqual(
            result["Compensation_Eligibility_Rules_group"],
            [
                {"DBS": "Y", "Bonus_Plan": "Commercial Bonus", "Target_Percent": 15},
                {"DBS": "N", "Bonus_Plan": "Spot Bonus", "Target_Percent": 5.5},
            ],
        )

    def test_single_instance_group(self):
        record = dict(grouped_row, Compensation_Eligibility_Rules_group=[grouped_row["Compensation_Eligibility_Rules_group"][0]])

        result = _transform(record)

        self.assertEqual(len(result["Compensation_Eligibility_Rules_group"]), 1)

    def test_empty_group(self):
        result = _transform(dict(grouped_row, Compensation_Eligibility_Rules_group=[]))

        self.assertEqual(result["Compensation_Eligibility_Rules_group"], [])

    def test_group_omitted_from_row(self):
        record = {"Job_Code": "JC200", "Job_Profile": "Analyst"}

        result = _transform(record)

        self.assertNotIn("Compensation_Eligibility_Rules_group", result)

    def test_group_with_missing_sub_fields(self):
        record = dict(grouped_row, Compensation_Eligibility_Rules_group=[{"Bonus_Plan": "Commercial Bonus"}])

        result = _transform(record)

        self.assertEqual(result["Compensation_Eligibility_Rules_group"], [{"Bonus_Plan": "Commercial Bonus"}])

    def test_group_returned_as_object_instead_of_list_is_wrapped_in_list(self):
        record = dict(grouped_row, Compensation_Eligibility_Rules_group=grouped_row["Compensation_Eligibility_Rules_group"][0])

        result = _transform(record)

        self.assertEqual(
            result["Compensation_Eligibility_Rules_group"],
            [{"DBS": "Y", "Bonus_Plan": "Commercial Bonus", "Target_Percent": 15}],
        )

    def test_group_returned_as_null_is_kept_as_null(self):
        result = _transform(dict(grouped_row, Compensation_Eligibility_Rules_group=None))

        self.assertIsNone(result["Compensation_Eligibility_Rules_group"])

    def test_group_items_that_are_not_objects_still_fail(self):
        with self.assertRaises(SchemaMismatch):
            _transform(dict(grouped_row, Compensation_Eligibility_Rules_group=["not an object"]))

    def test_sub_field_with_workday_specific_type_is_read_as_string(self):
        rich_text_xsd = xsd.replace(
            '<xsd:element name="Bonus_Plan" type="xsd:string" minOccurs="0"/>',
            '<xsd:element name="Bonus_Plan" type="wd:RichText" minOccurs="0"/>',
        )
        schema = discover.generate_schema_for_report(rich_text_xsd)

        self.assertEqual(
            schema["properties"]["Compensation_Eligibility_Rules_group"]["items"]["properties"]["Bonus_Plan"],
            {"type": ["string", "null"]},
        )
        result = _transform(grouped_row, schema)
        self.assertEqual(result["Compensation_Eligibility_Rules_group"][0]["Bonus_Plan"], "Commercial Bonus")


class XsdTypeMappingTest(unittest.TestCase):
    def _element_schema(self, xsd_type):
        element = discover.ElementTree.fromstring(
            '<xsd:element xmlns:xsd="http://www.w3.org/2001/XMLSchema" name="Field" type="{}"/>'.format(xsd_type)
        )
        return discover._element_to_schema(element)

    def test_number_like_types_map_to_number(self):
        for xsd_type in ("xsd:decimal", "xsd:double", "xsd:float"):
            self.assertEqual(self._element_schema(xsd_type), {"type": ["number"]}, xsd_type)

    def test_integer_like_types_map_to_integer(self):
        for xsd_type in ("xsd:integer", "xsd:int", "xsd:long", "xsd:short"):
            self.assertEqual(self._element_schema(xsd_type), {"type": ["integer"]}, xsd_type)

    def test_unknown_types_map_to_string(self):
        for xsd_type in ("wd:RichText", "xsd:anyURI", "xsd:time"):
            self.assertEqual(self._element_schema(xsd_type), {"type": ["string"]}, xsd_type)


class GroupColumnSyncTest(unittest.TestCase):
    def test_grouped_report_json_is_streamed_and_written_with_sub_fields(self):
        body = json.dumps({"Report_Entry": [grouped_row, {"Job_Code": "JC200", "Job_Profile": "Analyst"}]}).encode()
        session = mock.Mock()
        session.get.return_value = _GetContext(_StreamingResponse(body))

        schema = _schema()
        stream = CatalogEntry(
            tap_stream_id="Job_Profiles",
            stream="Job_Profiles",
            schema=Schema.from_dict(schema),
            metadata=metadata.get_standard_metadata(schema, replication_method="FULL_TABLE"),
        )
        report = {"report_name": "Job_Profiles", "report_url": "https://example/report"}

        with mock.patch.object(client, "_session_for_config", return_value=(session, None)), \
                mock.patch.object(sync.singer, "write_message") as write_message, \
                mock.patch.object(sync.singer, "write_version"):
            count = sync.sync_report(report, stream, {"username": "u", "password": "p"})

        records = [call.args[0].record for call in write_message.call_args_list]
        self.assertEqual(count, 2)
        self.assertEqual(
            records[0]["Compensation_Eligibility_Rules_group"],
            [
                {"DBS": "Y", "Bonus_Plan": "Commercial Bonus", "Target_Percent": 15},
                {"DBS": "N", "Bonus_Plan": "Spot Bonus", "Target_Percent": 5.5},
            ],
        )
        self.assertNotIn("Compensation_Eligibility_Rules_group", records[1])


if __name__ == "__main__":
    unittest.main()
