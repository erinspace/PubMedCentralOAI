''' Consumer for PubMed Central
    Takes in both metadata in dc and pmc formats '''

from __future__ import unicode_literals

import time
from lxml import etree
from datetime import date, timedelta
from dateutil.parser import parse


import requests
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

DEFAULT_ENCODING = 'ISO-8859-1'

OUTPUT_ENCODING = 'utf8'

record_encoding = None


def copy_to_unicode(element):
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=OUTPUT_ENCODING)


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
    title = record.xpath('//dc:title', namespaces=NAMESPACES)

    if isinstance(title, list):
        title = title[0]
        if isinstance(title, etree._Element):
            title = title.text

    title = title.strip()
    return copy_to_unicode(title)


def get_properties(record):
    properties = {}
    properties['type'] = (
        record.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    properties['language'] = (
        record.xpath('//dc:language/node()', namespaces=NAMESPACES) or [''])[0]
    properties['rights'] = (
        record.xpath('//dc:rights/node()', namespaces=NAMESPACES) or [''])[0]

    return {key: copy_to_unicode(value) for key, value in properties.iteritems()}


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
    service_id = record.xpath(
        'ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
    identifiers = record.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    
    for item in identifiers:
        if 'http://dx.doi.org' in item:
            id_doi = item.replace('http://dx.doi.org/', '')
        if 'http://' in item and 'articles/PMC' not in item and 'dx.doi' not in item:
            id_url = item

    if id_url == '' and id_doi:
        id_url = 'http://dx.doi.org/' + id_doi

    return {
        'url': copy_to_unicode(id_url),
        'doi': copy_to_unicode(id_doi),
        'serviceID': copy_to_unicode(service_id)
    }


def get_description(record):
    description = (record.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0]
    return copy_to_unicode(description)


def get_tags(record):
    tags = record.xpath('//dc:subject/node()', namespaces=NAMESPACES)
    return [copy_to_unicode(tag.lower()) for tag in tags]


def get_date_updated(record):
    date_updated = record.xpath('//dc:date/node()', namespaces=NAMESPACES)[0]
    date_updated = parse(date_updated).isoformat()
    return copy_to_unicode(date_updated)


def get_date_created(record):
    date_created = record.xpath('//dc:date/node()', namespaces=NAMESPACES)[0]
    date_created = parse(date_created).isoformat()
    return copy_to_unicode(date_created)


def normalize(raw_doc):
    raw_doc = raw_doc.get('doc')
    record = etree.XML(raw_doc)

    normalized_dict = {
        'title': get_title(record),
        'contributors': get_contributors(record),
        'properties': get_properties(record),
        'description': get_description(record),
        'id': get_ids(record),
        'tags': get_tags(record),
        'source': NAME,
        'dateCreated': get_date_created(record),
        'dateUpdated': get_date_updated(record)
    }

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
