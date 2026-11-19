import time
import singer
from singer import metadata, utils
from .transform import WorkdayTransformer as Transformer
from .client import stream_report
from .discover import PARENT_KEY_PREFIX

LOGGER = singer.get_logger()


def _parent_key_fields(schema):
    """Return {group: {child_field: parent_field}} for parent keys declared on group items."""
    properties = schema.get("properties", {})
    fields = {}
    for name, prop in properties.items():
        item_properties = prop.get("items", {}).get("properties", {}) if prop.get("type") == "array" else {}
        mapping = {
            child: child[len(PARENT_KEY_PREFIX):]
            for child in item_properties
            if child.startswith(PARENT_KEY_PREFIX) and child[len(PARENT_KEY_PREFIX):] in properties
        }
        if mapping:
            fields[name] = mapping
    return fields


def add_parent_keys(record, parent_key_fields):
    for group, mapping in parent_key_fields.items():
        items = record.get(group)
        for item in items if isinstance(items, list) else [items]:
            if isinstance(item, dict):
                for child, parent in mapping.items():
                    item.setdefault(child, record.get(parent))
    return record


def sync_report(report, stream, config):
    report_url = report["report_url"]

    LOGGER.info('Syncing report "%s".', report_url)

    record_count = 0

    record = {}

    stream_version = int(time.time() * 1000)
    extraction_time = utils.now().isoformat()

    singer.write_version(stream.tap_stream_id, stream_version)

    schema = stream.schema.to_dict()
    parent_key_fields = _parent_key_fields(schema)

    with Transformer() as transformer:
        for record in stream_report(report_url, config):
            record = add_parent_keys(record, parent_key_fields)
            to_write = transformer.transform(record, schema, metadata.to_map(stream.metadata))
            to_write["_sdc_extracted_at"] = extraction_time
            record_message = singer.RecordMessage(stream.tap_stream_id, to_write, version=stream_version)
            singer.write_message(record_message)
            record_count += 1

    return record_count
