''' Consumer for PubMed Central
    Takes in both metadata in dc and pmc formats '''

from lxml import etree
from datetime import date, timedelta
import requests
import time
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = "pubmedcentraloai"

NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/',
            'arch': 'http://dtd.nlm.nih.gov/2.0/xsd/archivearticle'}

def consume(days_back=0):
    start_date = TODAY - timedelta(days_back)
    base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords' 
    oai_dc_request = base_url + '&metadataPrefix=oai_dc&from={}'.format(str(start_date)) 

    # just for testing
    #print 'oai_dc request: ' + oai_dc_request

    oai_records = get_records(oai_dc_request)
    records = oai_records
    print '{} records collected...'.format(len(records))

    xml_list = []
    for record in records:
        ## TODO: make lack of contributors continue the loop
        contributors = record.xpath('//dc:creator/node()', namespaces=NAMESPACES) #changed
        if not contributors:
            continue
        doc_id = record.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
        record = etree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': doc_id,
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
        base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&resumptionToken=' 
        url = base_url + token[0]
        records += get_records(url)

    return records

def get_title(doc):
    title = doc.xpath('//dc:title', namespaces=NAMESPACES)

    if isinstance(title, list):
        title = title[0]
        if isinstance(title, etree._Element):
            title = title.text

    title = title.strip()
    return title

def get_properties(doc, metadata_type):
    properties = {}
    properties['type'] = (doc.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    properties['language'] = (doc.xpath('//dc:language/node()', namespaces=NAMESPACES) or [''])[0]
    properties['rights'] = (doc.xpath('//dc:rights/node()', namespaces=NAMESPACES) or [''])[0]
 
    return properties

def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)

    metadata_type = 'oai_dc'

    # properties #
    properties = get_properties(doc, metadata_type)

    ## title ##
    title = get_title(doc)

    ## contributors ##
    contributors = doc.xpath('//dc:creator/node()', namespaces=NAMESPACES)

    contributor_list = []
    for contributor in contributors: 
        if type(contributor) == tuple:
            contributor_list.append({'full_name': contributor[0], 'email':contributor[1]})
        else:
            contributor_list.append({'full_name': contributor, 'email':''})

    contributor_list = contributor_list or [{'full_name': 'no contributors', 'email': ''}]

    ## description ##
    description = doc.xpath('//dc:description/node()', namespaces=NAMESPACES)
    try:
        description = description[0]
    except IndexError:
        description = 'No description available.'

    ## id ##  
    id_url = ''
    id_doi = ''
    pmid = ''
    service_id = doc.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
    identifiers = doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    if len(identifiers) > 1: # there are multiple identifiers
        id_url = identifiers[1]
        if id_url[:17] == 'http://dx.doi.org':
            id_doi = id_url[18:]
        else:
            pmid = id_url[-8:]

    if len(identifiers) == 3:  # there are exactly three identifiers
        id_doi = identifiers[2][18:]

    if len(identifiers) == 1:
        raise Exception("No url provided!")

    doc_ids = {'url': id_url, 'doi': id_doi, 'service_id': service_id}

    ## tags ##
    tags = doc.xpath('//dc:subject/node()', namespaces=NAMESPACES)

    ## date created ##
    date_created = doc.xpath('//dc:date/node()', namespaces=NAMESPACES)[0]

    normalized_dict = { 
        'title': title,
        'contributors': contributor_list,
        'properties': properties,
        'description': description,
        'meta': {},
        'id': doc_ids,
        'tags': tags,
        'source': NAME,
        'date_created': date_created,
        'timestamp': str(timestamp)
    }

    return NormalizedDocument(normalized_dict)

if __name__ == '__main__':
    print(lint(consume, normalize))
