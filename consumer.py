''' Consumer for PubMed Central
    Takes in both metadata in dc and pmc formats '''

from __future__ import unicode_literals

import os
import time
from lxml import etree
from datetime import date, timedelta, datetime

import requests

from dateutil.parser import *

from nameparser import HumanName

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = "pubmed"

OAI_DC_BASE_URL = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords'

NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/',
              'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
              'ns0': 'http://www.openarchives.org/OAI/2.0/',
              'arch': 'http://dtd.nlm.nih.gov/2.0/xsd/archivearticle'}

DEFAULT_ENCODING = 'utf-8'

record_encoding = None

def copy_to_unicode(element):

    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)


def consume(days_back=0):
    start_date = TODAY - timedelta(days_back)
    oai_dc_request = OAI_DC_BASE_URL + \
        '&metadataPrefix=oai_dc&from={}'.format(str(start_date))
    record_encoding = requests.get(oai_dc_request).encoding

    # just for testing
    print 'oai_dc request: ' + oai_dc_request

    oai_records = get_records(oai_dc_request)
    records = oai_records
    print '{} records collected...'.format(len(records))

    xml_list = []
    for record in records:
        # TODO: make lack of contributors continue the loop
        contributors = record.xpath(
            '//dc:creator/node()', namespaces=NAMESPACES)  # changed
        if not contributors:
            continue
        doc_id = record.xpath(
            'ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
        record = etree.tostring(record, encoding=record_encoding)
        xml_list.append(RawDocument({
            'doc': record,
            'source': NAME,
            'docID': copy_to_unicode(doc_id),
            'filetype': 'xml'
        }))

    return xml_list


def get_records(url):
    data = requests.get(url)
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)

    if len(token) == 1:
        time.sleep(0.5)
        base_url = OAI_DC_BASE_URL + '&resumptionToken='
        url = base_url + token[0]
        records += get_records(url)

    return records


def get_title(record):
    title = doc.xpath('//dc:title', namespaces=NAMESPACES)

    if isinstance(title, list):
        title = title[0]
        if isinstance(title, etree._Element):
            title = title.text

    title = title.strip()
    return title


def get_properties(record):
    properties = {}
    properties['type'] = (
        doc.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    properties['language'] = (
        doc.xpath('//dc:language/node()', namespaces=NAMESPACES) or [''])[0]
    properties['rights'] = (
        doc.xpath('//dc:rights/node()', namespaces=NAMESPACES) or [''])[0]

    return properties


def get_contributors(record):
    contributors = record.xpath('//dc:creator/node()', namespaces=NAMESPACES)
    contributor_list = []
    for person in contributors:
        name = HumanName(person)
        contributor = {
            'prefix': name.title,
            'given': name.first,
            'middle': name.middle,
            'family': name.last,
            'suffix': name.suffix,
            'email': '',
            'ORCID': '',
        }
        contributor_list.append(contributor)
    return contributor_list


def get_ids(record):
    id_url = ''
    id_doi = ''
    pmid = ''
    service_id = doc.xpath(
        'ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
    identifiers = doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    if len(identifiers) > 1:  # there are multiple identifiers
        id_url = identifiers[1]
        if id_url[:17] == 'http://dx.doi.org':
            id_doi = id_url[18:]
        else:
            pmid = id_url[-8:]

    if len(identifiers) == 3:  # there are exactly three identifiers
        id_doi = identifiers[2][18:]

    if len(identifiers) == 1:
        raise Exception("No url provided!")

    return {'url': id_url, 'doi': id_doi, 'service_id': service_id}

def get_description(record):
    return (doc.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0]


def get_tags(record):
    return doc.xpath('//dc:subject/node()', namespaces=NAMESPACES)


def get_date_updated(record):
     doc.xpath('//dc:date/node()', namespaces=NAMESPACES)[0]


def get_date_created(record):
    doc.xpath('//dc:date/node()', namespaces=NAMESPACES)[0]


def normalize(raw_doc):
    raw_doc = raw_doc.get('doc')
    record = etree.XML(raw_doc)

    normalized_dict = {
        'title': get_title(record),
        'contributors': get_contributors(record),
        'properties': get_properties(record),
        'description': get_description(record),
        'id': get_ids(record),
        'tags': tags,
        'source': NAME,
        'dateCreated': get_date_created(record)
    }

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
