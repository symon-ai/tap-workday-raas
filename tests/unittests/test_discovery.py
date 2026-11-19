import unittest
from unittest import mock

from tap_workday_raas import discover


xsd = """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:wd="urn:com.workday.report/Stitch_Testing_2" xmlns:nyw="urn:com.netyourwork/aod" elementFormDefault="qualified" attributeFormDefault="qualified" targetNamespace="urn:com.workday.report/Stitch_Testing_2">
    <xsd:element name="Report_Data" type="wd:Report_DataType"/>
    <xsd:simpleType name="RichText">
        <xsd:restriction base="xsd:string"/>
    </xsd:simpleType>
    <xsd:complexType name="Candidate_Details_groupType">
        <xsd:sequence>
            <xsd:element name="Employee" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Willing_To_Travel" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Potential" type="xsd:string" minOccurs="0"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="Report_EntryType">
        <xsd:sequence>
            <xsd:element name="Default_Job_Title" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Average_Pay_-_Amount" type="xsd:decimal" minOccurs="0"/>
            <xsd:element name="job_profile_id" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Languages" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Default_Assessment_Tests" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Business_Unit_or_Business_Unit_Hierarchy_Container" type="xsd:string" minOccurs="0"/>
            <xsd:element name="Candidate_Details_group" type="wd:Candidate_Details_groupType" minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="Report_DataType">
        <xsd:sequence>
            <xsd:element name="Report_Entry" type="wd:Report_EntryType" minOccurs="0" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

class DiscoveryTest(unittest.TestCase):


    def test_generate_schema_for_report(self):

        expected = {'properties':
                    {'Average_Pay_-_Amount': {
                                              'type': ['number', 'null']},
                     'Business_Unit_or_Business_Unit_Hierarchy_Container': {'type': ['string', 'null']},
                     'Candidate_Details_group': {'items':
                                                 {'properties': {'Employee': {'type': ['string', 'null']},
                                                                 'Potential': {'type': ['string', 'null']},
                                                                 'Willing_To_Travel': {'type': ['string', 'null']}},
                                                  'type': 'object'},
                                                 'type': 'array'},
                     'Default_Assessment_Tests': {'type': ['string', 'null']},
                     'Default_Job_Title': {'type': ['string', 'null']},
                     'Languages': {'type': ['string', 'null']},
                     'job_profile_id': {'type': ['string', 'null']}},
                    'type': 'object'}

        actual = discover.generate_schema_for_report(xsd)
        
        self.assertEqual(expected, actual)

    @mock.patch.object(discover, "download_xsd", return_value=xsd)
    @mock.patch.object(discover, "_session_for_config")
    def test_discover_reuses_single_session_for_all_reports(
        self, mock_session_for_config, mock_download_xsd
    ):
        session = mock.Mock()
        oauth_provider = mock.Mock()
        mock_session_for_config.return_value = (session, oauth_provider)
        config = {
            "auth_type": "oauth",
            "client_id": "id",
            "client_secret": "sec",
            "token_url": "https://example/token",
            "refresh_token": "rt",
            "reports": [
                {"report_name": "Report_A", "report_url": "https://example/a"},
                {"report_name": "Report_B", "report_url": "https://example/b"},
                {"report_name": "Report_C", "report_url": "https://example/c"},
            ],
        }

        streams = discover.discover_streams(config)

        mock_session_for_config.assert_called_once_with(config)
        self.assertEqual(mock_download_xsd.call_count, 3)
        self.assertEqual(len(streams), 3)
        for call in mock_download_xsd.call_args_list:
            self.assertEqual(call.kwargs["session"], session)
            self.assertEqual(call.kwargs["oauth_provider"], oauth_provider)
