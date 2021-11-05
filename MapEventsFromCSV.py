#!/usr/bin/env python3

'''
Author:
    Liher Elgezabal <liher.elgezabal@es.ibm.com>

Date:
    2019-06-05

Version:
    1.0

Description:
    A script to create new QIDs and map them.
    The input is a CSV file with the mapping and the QID record values
    The output is a CSV file with the result of the import and the error messages for each record.
    If validation or lookup errors are found in a line, an error message will be shown in console
    logging file can be reviewed for further details. Verbose option will also show the REST API queries.

    This script is written by the IBM Product Professional Services team.
    This is not a supported script. Use at your own risk.

Usage:
    Usage: MapEventsFromCSV.py [options]

    Options:
      -h, --help            show this help message and exit
      -i FILE, --input_file=FILE
                            Input CSV file name.
      -o FILE, --output_file=FILE
                            Output CSV file name.
      -l FILE, --log_file=FILE
                            log file name.
      -v, --verbose         Enable verbose messages
      -d DEBUG, --debug=DEBUG
                            Define logging level.
                            [DEBUG|INFO|WARNING|ERROR*|CRITICAL]


Changelog:
    v1.0 - 2019-06-05 + Initial script

To Do:
    + find the unique value based on the HLC
    + extract categories
'''

import importlib
import json
import os
import sys
import optparse
import csv
import xml
import logging

sys.path.append(os.path.realpath('modules'))
client_module = importlib.import_module('RestApiClient')

def main(input_file):

    with open(input_file, mode='r') as input_file_handler: # open file
        csv_reader = csv.DictReader(input_file_handler)
        for csv_line in csv_reader: # for each line
                logging.info(csv_line)
                try:
                    valid_csv_line = validate_csv_line(csv_line) # validate content
                except ValueError as error: # if controlled validation error (log to console and skip this line)
                     logging.error(error)
                     logging.error("Line: " + str(csv_line))
                     print(str(error) + "\nLine:\n" + str(csv_line))
                     result=dict()
                     result.update(csv_line)
                     result.update({"mapping_result": "ERROR", "mapping_result_msg": error})
                     results.append(result)
                except LookupError as error: # if controlled lookup error (log to console and skip this line)
                     logging.error(error)
                     logging.error("Line: " + str(csv_line))
                     print(str(error) + "\nLine:\n" + str(csv_line))
                     result=dict()
                     result.update(csv_line)
                     result.update({"mapping_result": "ERROR", "mapping_result_msg": error})
                     results.append(result)
                else: # process line
                    process_csv_line(valid_csv_line)


def csv_line_contains_value_for(csv_line, field):
    return field in csv_line and csv_line[field] != "" and csv_line[field] != None

def validate_csv_line(csv_line):

    # Get Log Source Type ID
    if not csv_line_contains_value_for(csv_line,"Log Source Type ID"):
        if csv_line_contains_value_for(csv_line,"Log Source Type"):
            csv_line["Log Source Type ID"] = get_log_source_type_id(csv_line["Log Source Type"])
        else:
            raise ValueError('Log Source Type or Log Source Type ID must be provided')

    if csv_line_contains_value_for(csv_line,"QID") and (csv_line_contains_value_for(csv_line,"QID Name") or
        csv_line_contains_value_for(csv_line,"QID Description") or
        csv_line_contains_value_for(csv_line,"Severity") or
        csv_line_contains_value_for(csv_line,"Low Level Category ID")):
        raise ValueError('Provide either existing QID or New QID Values but not both')


    # Get Low Level Category ID
    if not csv_line_contains_value_for(csv_line,"Low Level Category ID"):
        if csv_line_contains_value_for(csv_line,"Low Level Category"):
            csv_line["Low Level Category ID"] = get_low_level_category_id(csv_line["Low Level Category"], csv_line["High Level Category"])
        else:
            raise ValueError('Low Level Category or Low Level Category ID must be provided')

    # get severity
    if not csv_line_contains_value_for(csv_line,"Severity"):
        csv_line["Severity"] = get_default_severity(csv_line["Low Level Category ID"])

    # get QID Descrition
    if not csv_line_contains_value_for(csv_line,"QID Description"):
        csv_line["QID Description"] = ""

    return csv_line

def get_log_source_type_id(log_source_type):
    # prepare request
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = 'config/event_sources/log_source_management/log_source_types'
    http_method = 'GET'
    fields = 'id, name, custom'
    query_filter = 'name ilike "'+log_source_type+'" '
    params = {'fields': fields, 'filter': query_filter}
    headers = {'range': 'items=0-5'}

    # send the request
    response = client.call_api(endpoint_url, http_method, params=params,
                               headers=headers, print_request=options.verbose)

    # handle response
    if response.code == 200:
        qid_records = json.loads(response.read().decode('utf-8'))
        # go through the returned list of qid records and print each one
        logging.debug (qid_records)
        for qid_record in qid_records:
            logging.debug(qid_record)
        if len(qid_records) == 1:
            return qid_records[0]["id"]
        elif len(qid_records) == 0:
            raise LookupError('Could not find any Log Source Type ID for Log Source Type ' + log_source_type)
        else:
            raise LookupError('Found '+ str(len(qid_records)) + ' records for Log Source Type ' + log_source_type)
    else:
        logging.error(pretty_print_response(response))
        raise LookupError('Failed to retrieve the list of log source type records')

def get_default_severity(low_level_category_id):
    # prepare request
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = 'data_classification/low_level_categories' + '/' + str(low_level_category_id)
    http_method = 'GET'


    # send the request
    response = client.call_api(endpoint_url, http_method, print_request=options.verbose)

    # handle response
    if response.code == 200:
        llc_record = json.loads(response.read().decode('utf-8'))
        # go through the returned list of qid records and print each one
        logging.debug(llc_record)
        if "severity" in llc_record:
            logging.debug(llc_record)
            return str(llc_record["severity"])
        else:
            raise LookupError('Could not find any Low Level Category ID ' + low_level_category_id)
    else:
        raise LookupError('Failed to retrieve Low Level Category record')

def get_low_level_category_id(low_level_category, high_level_category):
    # prepare request
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = 'data_classification/low_level_categories'
    http_method = 'GET'
    fields = 'id, name, description, severity, high_level_category_id'
    query_filter = 'name ilike "'+low_level_category+'" '
    params = {'fields': fields, 'filter': query_filter}
    headers = {'range': 'items=0-5'}

    # send the request
    response = client.call_api(endpoint_url, http_method, params=params,
                               headers=headers, print_request=options.verbose)

    # handle response
    if response.code == 200:
        llc_records = json.loads(response.read().decode('utf-8'))
        # go through the returned list of qid records and print each one
        logging.debug(llc_records)
        for llc_record in llc_records:
            logging.debug(llc_record)
        if len(llc_records) == 1:
            return llc_records[0]["id"]
        elif len(llc_records) == 0:
            raise LookupError('Could not find any Low Level Category ID for Low Level Category ' + low_level_category)
        else:
            # TODO: find the unique value based on the HLC
            raise LookupError('Found '+ str(len(llc_records)) + ' records for Low Level Category ' + low_level_category)
    else:
        logging.error(pretty_print_response(response))
        raise LookupError('Failed to retrieve the list of Low Level Category records')

def process_csv_line(csv_line):

    # create a new qid record first to be mapped to the dsm event
    new_qid_record = {'log_source_type_id': int(csv_line["Log Source Type ID"]),
                      'name': csv_line["QID Name"],
                      'description': csv_line["QID Description"],
                      'severity': int(csv_line["Severity"]),
                      'low_level_category_id': int(csv_line["Low Level Category ID"])
                      }

    dsm_event_mapping = get_dsm_event_mapping(csv_line)

    # first take care of QID record
    if csv_line_contains_value_for(csv_line,"QID") and csv_line["QID"] != "0": # use provided QID for mapping
        qid_records = get_qid_records('qid = ' + str(csv_line["QID"]))
        if len(qid_records) == 0:
            msg="Can't find QID record for QID " + str(csv_line["QID"]) + ". do nothing!"
            logging.info(msg)
            qid_record = {"qid_result": "SKIPPED", "qid_result_msg": msg}
        elif len(qid_records) == 1:
            qid_record=qid_records[0]
            msg="using existing QID record! " + str(csv_line["QID"])
            logging.info(msg)
            qid_record.update({"qid_result": "SKIPPED", "qid_result_msg": msg})
        else:
            msg="Found dupplicated QID records for QID " + str(csv_line["QID"]) + ". This should never happen do nothing!"
            logging.info(msg)
            qid_record = {"qid_result": "SKIPPED", "qid_result_msg": msg}
    elif dsm_event_mapping != None: # use currently mapped QID and update it with new values
        logging.debug(new_qid_record)
        qid_record = update_qid_record(dsm_event_mapping["qid_record_id"],new_qid_record)
    else: # create new qid record and dsm_event_mapping
        logging.debug(new_qid_record)
        qid_record = create_qid_record(new_qid_record)

    # then take care of event mapping
    if dsm_event_mapping == None: # create new mapping
        new_dsm_event_mapping = {"log_source_type_id": int(csv_line["Log Source Type ID"]),
                                 "log_source_event_id": csv_line["Event ID"],
                                 "log_source_event_category": csv_line["Event Category"],
                                 "qid_record_id": qid_record['id']
                                 }
        logging.debug(new_dsm_event_mapping)
        dsm_event_mapping = create_dsm_event_mapping(new_dsm_event_mapping)
    elif 'id' not in qid_record:
        msg="QID not available"
        logging.info(msg)
        dsm_event_mapping.update({"mapping_result": "SKIPPED", "mapping_result_msg": msg})
    elif dsm_event_mapping['qid_record_id'] != qid_record['id']: # update existing mapping (if needed)
        dsm_event_mapping = update_dsm_event_mapping(dsm_event_mapping["id"],{"qid_record_id": qid_record['id']})
    else: # skip mapping
        msg="Already mapped"
        logging.info(msg)
        dsm_event_mapping.update({"mapping_result": "SKIPPED", "mapping_result_msg": msg})

    result=dict()
    #result.update(csv_line)
    result.update(qid_record)
    result.update(dsm_event_mapping)
    #print(qid_record)
    #print(dsm_event_mapping)
    #print(result)

    results.append(result)

def write_file(output_file, results):
        """
        Writes the passed results to the output file.

        results must be a list of result objects this is (I guess pretty loosely) defined
        in this script. output_file is the name of the file to write the results to.

        The header_row controls the ordering of the output results. The function also
        maps the keys in the result objects to the header rows so it can be written
        using the csv.DictReader method.
        """
        header_row = [
            'Event Mapping Record ID',
            'Log Source Type',
            'Log Source Type ID',
            'Event Category',
            'Event ID',
            'QID Record ID',
            'QID',
            'QID Name',
            'QID Description',
            'Severity',
            'Low Level Category ID',
            'Low Level Category',
            'High Level Category',
            'Mapping Result',
            'Mapping Result Msg',
            'QID Result',
            'QID Result Msg'
        ]

        header_mapping = {
            'id': 'Event Mapping Record ID',
            'log_source_type': 'Log Source Type',
            'log_source_type_id': 'Log Source Type ID',
            'log_source_event_category': 'Event Category',
            'log_source_event_id': 'Event ID',
            'qid_record_id': 'QID Record ID',
            'qid': 'QID',
            'name': 'QID Name',
            'description': 'QID Description',
            'severity': 'Severity',
            'low_level_category_id': 'Low Level Category ID',
            'low_level_category': 'Low Level Category',
            'high_level_category': 'High Level Category',
            'mapping_result': 'Mapping Result',
            'mapping_result_msg': 'Mapping Result Msg',
            'qid_result': 'QID Result',
            'qid_result_msg': 'QID Result Msg'
        }

        for result in results:
            for mapping in header_mapping.items():
                if mapping[0] in result:
                    result[mapping[1]] = result.pop(mapping[0])

        with open(output_file, 'w', encoding='utf-8-sig', newline="") as output_file_handle:
            writer = csv.DictWriter(f=output_file_handle, fieldnames=header_row, extrasaction='ignore', dialect='excel', delimiter=',')
            writer.writerow(dict((h, h) for h in header_row))
            writer.writerows([dict((k, v) for k, v in result.items()) for result in results])


def pretty_print_response(response):
    return json.dumps(json.loads(response.read().decode('utf-8')), indent=4)

def get_dsm_event_mapping(csv_line):

    # prepare request
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = 'data_classification/dsm_event_mappings'
    http_method = 'GET'
    fields = 'id, log_source_type_id, log_source_event_id, log_source_event_category, qid_record_id'
    query_filter = 'log_source_type_id = ' + str(csv_line["Log Source Type ID"]) + ' and log_source_event_id = "' + str(csv_line["Event ID"]) + '" and log_source_event_category = "' + str(csv_line["Event Category"]) +'"'
    params = {'fields': fields, 'filter': query_filter}
    headers = {'range': 'items=0-5'}

    # send the request
    response = client.call_api(endpoint_url, http_method, params=params,
                               headers=headers, print_request=options.verbose)

    # handle response
    if response.code == 200:
        dsm_event_mapping_records = json.loads(response.read().decode('utf-8'))
        # go through the returned list of qid records and print each one
        for dsm_event_mapping_record in dsm_event_mapping_records:
            logging.debug(dsm_event_mapping_record)
        if len(dsm_event_mapping_records) > 0:
            return dsm_event_mapping_records[0]

    else:
        logging.error(pretty_print_response(response))
        raise LookupError('Failed to retrieve the list of dsm_event_mappings')

def get_qid_record(qid_record_id):
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = ('data_classification/qid_records' + '/' + str(qid_record_id))
    http_method = 'GET'
    response = client.call_api(endpoint_url, http_method, print_request=options.verbose)

    # check response and handle any error
    if response.code == 200:
        qid_record = json.loads(response.read().decode('utf-8'))
        logging.debug(json.dumps(qid_record, indent=4))
        return qid_record
    else:
        SampleUtilities.pretty_print_response(response)
        raise LookupError('Failed to retrieve the qid record with id=' + str(qid_record_id))

def get_qid_records(query_filter):

    # prepare request
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = 'data_classification/qid_records'
    http_method = 'GET'
    fields = 'id, qid, name, description, severity, low_level_category_id'
    # query_filter = 'name ilike "%authentication%" '
    params = {'fields': fields, 'filter': query_filter}
    headers = {'range': 'items=0-5'}

    # send the request
    response = client.call_api(endpoint_url, http_method, params=params,
                               headers=headers, print_request=options.verbose)

    # handle response
    if response.code == 200:
        qid_records = json.loads(response.read().decode('utf-8'))
        # go through the returned list of qid records and print each one
        for qid_record in qid_records:
            logging.debug(qid_record)
        return qid_records

    else:
        logging.error(pretty_print_response(response))
        raise LookupError('Failed to retrieve the list of qid records')

def update_dsm_event_mapping(dsm_event_mapping_id, fields_to_update):

    # prepare request
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = ('data_classification/dsm_event_mappings' + '/' + str(dsm_event_mapping_id))
    http_method = 'POST'
    '''
    fields_to_update = {'name': 'an updated qid record name',
                        'severity': 8
                        }
    '''
    data = json.dumps(fields_to_update).encode('utf-8')
    headers = {'Content-type': 'application/json'}

    # send the request
    response = client.call_api(endpoint_url, http_method, data=data,
                               headers=headers, print_request=options.verbose)

    # check response and handle any error
    if response.code == 200:
        updated_dsm_event_mapping = json.loads(response.read().decode('utf-8'))
        logging.info(json.dumps(updated_dsm_event_mapping, indent=4))
        updated_dsm_event_mapping.update({"mapping_result": "UPDATED"})
        return updated_dsm_event_mapping
    else:
        msg = 'Failed to update the mapping record with id=' + str(dsm_event_mapping_id)
        logging.error(msg)
        response_txt = pretty_print_response(response)
        logging.error(response_txt)
        return {"mapping_result": "FAILED_UPDATE", "mapping_result_msg": json.loads(response_txt)['description']}


# function helps creating a new dsm event mapping
def create_dsm_event_mapping(dsm_event_mapping):

    # prepare request
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = 'data_classification/dsm_event_mappings'
    http_method = 'POST'
    data = json.dumps(dsm_event_mapping).encode('utf-8')
    headers = {'Content-type': 'application/json'}

    # send the request
    response = client.call_api(endpoint_url, http_method, data=data,
                               headers=headers, print_request=options.verbose)

    # check response and handle any error
    if response.code == 201:
        dsm_event_mapping = json.loads(response.read().decode('utf-8'))
        logging.info('A new dsm event mapping is created. ID: ' + str(dsm_event_mapping["id"]))
        logging.debug(json.dumps(dsm_event_mapping, indent=4))
        dsm_event_mapping.update({"mapping_result": "CREATED"})
        return dsm_event_mapping
    else:
        msg = 'Failed to create the new dsm event mapping'
        logging.error(msg)
        response_txt = pretty_print_response(response)
        logging.error(response_txt)
        return {"mapping_result": "FAILED_CREATE", "mapping_result_msg": json.loads(response_txt)['description']}

def update_qid_record(qid_record_id, fields_to_update):

    # prepare request
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = ('data_classification/qid_records' + '/' + str(qid_record_id))
    http_method = 'POST'
    '''
    fields_to_update = {'name': 'an updated qid record name',
                        'severity': 8
                        }
    '''
    data = json.dumps(fields_to_update).encode('utf-8')
    headers = {'Content-type': 'application/json'}

    # send the request
    response = client.call_api(endpoint_url, http_method, data=data,
                               headers=headers, print_request=options.verbose)

    # check response and handle any error
    if response.code == 200:
        updated_qid_record = json.loads(response.read().decode('utf-8'))
        logging.info(json.dumps(updated_qid_record, indent=4))
        updated_qid_record.update({"qid_result": "UPDATED"})
        return updated_qid_record
    else:
        msg = 'Failed to update the qid record with id=' + str(qid_record_id)
        logging.error(msg)
        response_txt = pretty_print_response(response)
        logging.error(response_txt)
        return {"qid_result": "FAILED_UPDATE", "qid_result_msg": json.loads(response_txt)['description']}



# function helps creating qid record needed for dsm event mapping
def create_qid_record(qid_record):

    # prepare request
    client = client_module.RestApiClient(version='15.1')
    endpoint_url = 'data_classification/qid_records'
    http_method = 'POST'
    data = json.dumps(qid_record).encode('utf-8')
    headers = {'Content-type': 'application/json'}

    # send the request
    response = client.call_api(endpoint_url, http_method, data=data,
                               headers=headers, print_request=options.verbose)

    # check response and handle any error
    if response.code == 201:
        qid_record = json.loads(response.read().decode('utf-8'))
        logging.info('A new qid record is created. ID: ' + str(qid_record["id"]))
        qid_record.update({"qid_result": "CREATED"})
        return qid_record
    else:
        msg = 'Failed to create the new qid record'
        logging.error(msg)
        response_txt = pretty_print_response(response)
        logging.error(response_txt)
        return {"qid_result": "FAILED_CREATE", "qid_result_msg": json.loads(response_txt)['description']}


def parse_arguments(arguments):
    """
    Parse the arguments passed to the script.

    Returns the parsed options.
    """
    parser = optparse.OptionParser(usage='usage: %prog [options]')

    parser.add_option('-i',
                      '--input_file',
                      dest='input_file',
                      action='store',
                      help='Input CSV file name.',
                      metavar='FILE'
                      )


    parser.add_option('-o',
                      '--output_file',
                      dest='output_file',
                      action='store',
                      help='Output CSV file name.',
                      metavar='FILE'
                      )

    parser.add_option('-l',
                      '--log_file',
                      dest='log_file',
                      action='store',
                      default='log/' + os.path.basename(__file__) + '.log',
                      help='log file name.',
                      metavar='FILE'
                      )

    parser.add_option('-v',
                      '--verbose',
                      dest='verbose',
                      action='store_true',
                      default=False,
                      help='Enable verbose messages',
                      )

    parser.add_option('-d',
                      '--debug',
                      dest='debug',
                      action='store',
                      default=4,
                      metavar='LEVEL',
                      help='Define logging level. [DEBUG|INFO|WARNING|ERROR*|CRITICAL]',
                      )


    if not arguments:
        print ("No arguments found.")
        parser.print_help()
        sys.exit(-1)

    (options, args) = parser.parse_args()

    if not options.input_file:
        print ("No input CSV file specified.")
        parser.print_help()
        sys.exit(-1)

    if not options.output_file:
        print ("No output CSV file specified.")
        parser.print_help()
        sys.exit(-1)

    if(options.debug == 'DEBUG'):
        options.debug=logging.DEBUG
    elif (options.debug == 'INFO'):
        options.debug=logging.INFO
    elif (options.debug == 'WARNING'):
        options.debug=logging.WARNING
    elif (options.debug == 'ERROR'):
        options.debug=logging.ERROR
    elif (options.debug == 'CRITICAL'):
        options.debug=logging.CRITICAL
    else:
        options.debug=logging.ERROR

    return options


if __name__ == '__main__':
    results=[]
    options = parse_arguments(sys.argv[1:])
    logging.basicConfig(filename=options.log_file,level=options.debug, format='%(asctime)s - %(levelname)s - %(message)s')
    main(options.input_file)
    write_file(options.output_file,results)
