import io
import time
import random
from datetime import datetime
import os
import csv
from xlsx2csv import Xlsx2csv

import singer
from googleapiclient import http

from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    REPORT_ID_FIELD,
    get_fields,
    get_schema,
    get_field_type_lookup
)

LOGGER = singer.get_logger()

MIN_RETRY_INTERVAL = 2 # 10 seconds
MAX_RETRY_INTERVAL = 300 # 5 minutes
MAX_RETRY_ELAPSED_TIME = 3600 # 1 hour
CHUNK_SIZE = 16 * 1024 * 1024 # 16 MB

def next_sleep_interval(previous_sleep_interval):
    min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
    max_interval = previous_sleep_interval * 2 or MIN_RETRY_INTERVAL
    return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))

def parse_line(line):
    with io.StringIO(line) as stream:
        reader = csv.reader(stream)
        return next(reader)

def transform_field(dfa_type, value):
    if value == '':
        return None
    if dfa_type == 'double':
        return float(value)
    if dfa_type == 'long':
        try:
            return int(value)
        except:
            return None
    if dfa_type == 'boolean':
        value = value.lower().strip()
        return (
            value == 'true' or
            value == 't' or
            value == 'yes' or
            value == 'y'
        )
    return value

def process_file(service, fieldmap, report_config, file_id, report_time):
    working_dir = '/tmp/'
    out_file = io.FileIO(os.path.join(working_dir,report_config['stream_name']+'.xlsx'), mode='wb')

    # Create a get request.
    request = service.files().get_media(reportId=report_config['report_id'], fileId=file_id)

    # Create a media downloader instance.
    # Optional: adjust the chunk size used when downloading the file.
    downloader = http.MediaIoBaseDownload(
        out_file, request, chunksize=CHUNK_SIZE)

    # Execute the get request and download the file.
    download_finished = False
    while download_finished is False:
        _, download_finished = downloader.next_chunk()

    print('File %s downloaded to %s' % (file_id,
                                        os.path.realpath(out_file.name)))

    csv_file = os.path.join(working_dir, report_config['stream_name'] + '.csv')

    Xlsx2csv(os.path.realpath(out_file.name),  outputencoding="utf-8").convert(csv_file)

    report_id = report_config['report_id']
    stream_name = report_config['stream_name']
    stream_alias = report_config['stream_alias']

    line_state = {
        'headers_line': False,
        'past_headers': False,
        'count': 0
    }

    report_id_int = int(report_id)
    #
    def line_transform(line):
        if line.startswith((' ', '\t')):
            return
        if not line_state['past_headers'] and not line_state['headers_line'] and 'Report Fields' in line:
            line_state['headers_line'] = True
            return
        if line_state['headers_line']:
            line_state['headers_line'] = False
            line_state['past_headers'] = True
            return

        if line_state['past_headers']:
            row = parse_line(line)
            # skip report grant total line
            if row[0] == 'Grand Total:':
                return

            obj = {}
            for i in range(len(fieldmap)):
                field = fieldmap[i]
                obj[field['name']] = transform_field(field['type'], row[i])

            obj[SINGER_REPORT_FIELD] = report_time
            obj[REPORT_ID_FIELD] = report_id_int

            singer.write_record(stream_name, obj, stream_alias=stream_alias)
            line_state['count'] += 1

    with open(csv_file) as f:
        for line in f:
            line_transform(line)

    with singer.metrics.record_counter(stream_name) as counter:
        counter.increment(line_state['count'])

def sync_report(service, field_type_lookup, profile_id, report_config):
    report_id = report_config['report_id']
    stream_name = report_config['stream_name']
    stream_alias = report_config['stream_alias']

    LOGGER.info("%s: Starting sync", stream_name)

    report = (
        service
        .reports()
        .get(profileId=profile_id,
             reportId=report_id)
        .execute()
    )

    fieldmap = get_fields(field_type_lookup, report)
    schema = get_schema(stream_name, fieldmap)
    singer.write_schema(stream_name, schema, [], stream_alias=stream_alias)

    with singer.metrics.job_timer('run_report'):
        report_time = datetime.utcnow().isoformat() + 'Z'
        report_file = (
            service
            .reports()
            .run(profileId=profile_id,
                 reportId=report_id)
            .execute()
        )

        report_file_id = report_file['id']

        sleep = 0
        start_time = time.time()
        while True:
            report_file = (
                service
                .files()
                .get(reportId=report_id,
                     fileId=report_file_id)
                .execute()
            )

            status = report_file['status']
            if status == 'REPORT_AVAILABLE':
                process_file(service, fieldmap, report_config, report_file_id, report_time)
                break
            elif status != 'PROCESSING':
                message = '{}: report_id {} / file_id {} - File status is {}, processing failed'.format(
                        stream_name,
                        report_id,
                        report_file_id,
                        status)
                LOGGER.error(message)
                raise Exception(message)
                raise Exception(message)
            elif time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
                message = '{}: report_id {} / file_id {} - File processing deadline exceeded ({} secs)'.format(
                        stream_name,
                        report_id,
                        report_file_id,
                        status,
                        MAX_RETRY_ELAPSED_TIME)
                LOGGER.error(message)
                raise Exception(message)

            sleep = next_sleep_interval(sleep)
            LOGGER.info('{}: report_id {} / file_id {} - File status is {}, sleeping for {} seconds'.format(
                        stream_name,
                        report_id,
                        report_file_id,
                        status,
                        sleep))
            time.sleep(sleep)

def sync_reports(service, config, catalog, state):
    profile_id = config.get('profile_id')

    reports = []
    for stream in catalog.streams:
        mdata = singer.metadata.to_map(stream.metadata)
        root_metadata = mdata[()]
        if root_metadata.get('selected') is True:
            reports.append({
                'report_id': root_metadata['tap-doubleclick-campaign-manager.report-id'],
                'stream_name': stream.tap_stream_id,
                'stream_alias': stream.stream_alias
            })
    reports = sorted(reports, key=lambda x: x['report_id'])

    # if report selection changes, we want to start over
    if state.get('reports') != reports:
        state['current_report'] = None
        state['reports'] = reports

    field_type_lookup = get_field_type_lookup()

    current_report = state.get('current_report')
    past_current_report = False
    for report_config in reports:
        report_id = report_config['report_id']

        if current_report is not None and \
           not past_current_report and \
           current_report != report_id:
            continue

        past_current_report = True
        state['current_report'] = report_id
        singer.write_state(state)

        sync_report(service,
                    field_type_lookup,
                    profile_id,
                    report_config)

    state['reports'] = None
    state['current_report'] = None
    singer.write_state(state)
