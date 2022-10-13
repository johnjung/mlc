import io, json, os, re, rdflib, requests, sys, urllib
from flask import Flask, jsonify, render_template, request
from lxml import etree as etree

app = Flask(__name__)
app.config.from_pyfile('local.py')

MARKLOGIC_SERVER=app.config['MARKLOGIC_SERVER']
PROXY_SERVER=app.config['PROXY_SERVER']

if __name__ == '__main__':
    app.run(host='0.0.0.0')

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def glotto_labels(code):
    #with open('glotto.json') as f:
    with open(app.config['GLOTTO_JSON']) as f:
        lookup = json.load(f)
    try:
        return lookup[code]
    except KeyError:
        return ''

def get_facets(identifier_set, predicate_set):
    '''Get facets for a search result.

       Params:
         identifier_set: a set() of identifiers from non-paged search results.
                         Include facets for only these results. Individual
                         identifiers should be formatted like "b20v4130ft31".
         predicate_set:  a set() of predicates to return facets for.

       Returns:

       Notes:
         The query for all triples returns XML data like this:
   
         <sparql xmlns="http://www.w3.org/2005/sparql-results#">
           <head>  
             <variable name="s"/>
             <variable name="p"/>
             <variable name="o"/>
           </head> 
           <results>
             <result>
               <binding name="s">
                 <uri>https://www.lib.uchicago.edu/ark:61001/b20v4130ft31</uri>
               </binding>
               <binding name="p">
                 <uri>http://id.loc.gov/ontologies/bibframe/Place</uri>
               </binding>
               <binding name="o">
                 <literal>University of Chicago Library (Chicago, IL)</literal>
               </binding>
             </result>
             ...
           </results>
         </sparql>
   
         1) Extract the subject, predicate and object for each result.
         2) Be sure the subject is a URI ending in <noid>, because results also
            include things like "/agg" URIs.
         3) Convert the subject URI to a plain ARK, like "ark:61001/b20v4130ft31".
         4) Be sure the subject is include in the identifier_set passed to this
            function.
         5) Be sure the predicate is included in the predicate_set passed to
            this function.
    '''

    url = MARKLOGIC_SERVER + '/chas_query.xqy?query=all&collection=mila&format=xml'
    r = requests.get(url)
    xml = etree.fromstring(r.text)

    results = {}

    for result in xml.xpath(
        '//sparql:result',
        namespaces={'sparql': 'http://www.w3.org/2005/sparql-results#'}
    ):
        s = result.xpath(
            'sparql:binding[@name="s"]/sparql:*',
            namespaces={'sparql': 'http://www.w3.org/2005/sparql-results#'}
        )[0].text
        p = result.xpath(
            'sparql:binding[@name="p"]/sparql:*',
            namespaces={'sparql': 'http://www.w3.org/2005/sparql-results#'}
        )[0].text
        o = result.xpath(
            'sparql:binding[@name="o"]/sparql:*',
            namespaces={'sparql': 'http://www.w3.org/2005/sparql-results#'}
        )[0].text

        # Be sure the subject node is for a cho only:
        if not re.search('ark:61001/[a-z0-9]+$', s):
            continue

        # filter for relevant predicates.
        if p not in predicate_set:
            continue

        # use identifiers like "ark:61001/z90z41h24f07".
        r = re.search('ark:61001/([a-z0-9]+)$', s)
        identifier = r.group(1)

        if identifier not in identifier_set:
            continue

        if not identifier in results:
            results[identifier] = {}
        if not p in results[identifier]:
            results[identifier][p] = set()
        results[identifier][p].add(o)

    facets = {}
    for identifier, d in results.items():
        for p, o_set in d.items():
            for o in o_set:
                if not p in facets:
                    facets[p] = {}
                if not o in facets[p]:
                    facets[p][o] = set()
                facets[p][o].add(identifier)

    return facets


def process_search_results(xml):
    '''
      {
        'params': {
          'collection': 'mila',
          'facets': [
            'language/Bulgarian',
            'language/English',
            'location/Bulgaria',
            'access/By%20Request',
            'access/Login%20Required
          ],  
          'page': n,
          'page_size': n,
          'query_type': 'language' | 'spatial',
          'query': query_string,
        },  
        'facets': {
          'http://purl.org/dc/terms/rights': {
            'Public domain': [...],
            'Restricted': [...],
            'Campus': [...],
            'null': [...]
          }   
        },  
        'pager': {
          'result_count': n,
          'page_size': n,
          'page': n,
        },  
        'results': [
          {   
            'identifier': '',
            'title': '',
          },  
          ... 
        ]
      }
    '''
    results = []
    for result in xml.xpath(
        '//sparql:result',
        namespaces={'sparql': 'http://www.w3.org/2005/sparql-results#'}
    ):
        r = {}
        for b in (
            'creator',              # language spatial
            'date',                 # language spatial
            'identifier',           # language spatial, e.g. https://n2t.net/ark:61001/z90z41h24f07
            'invertedLanguageName', # language spatial
            'place',                #          spatial
            'resource',             #          spatial, (ignore this)
            'rights',               # language spatial
            'spatial',              #          spatial
            'subjectlanguage',      # language
            'tgn',                  #          spatial
            'title'                 # language spatial
        ):
            try:
                v = result.xpath(
                  'sparql:binding[@name="{}"]/sparql:*'.format(b), 
                  namespaces={'sparql': 'http://www.w3.org/2005/sparql-results#'}
                )[0].text
            except IndexError:
                continue
            # use identifiers like "z90z41h24f07".
            if b == 'identifier':
                v = v.replace('https://n2t.net/ark:61001/', '')
            r[b] = v
        results.append(r)

    identifier_set = set()
    for r in results:
        identifier_set.add(r['identifier'])

    predicate_set = set(['http://lib.uchicago.edu/ucla/invertedLanguageName', 'http://purl.org/dc/terms/rights'])

    facets = get_facets(identifier_set, predicate_set)

    return {'facets': facets, 'results': results}

@app.route('/')
def home():
    return render_template(
        'home.html'
    )

@app.route('/objectdata/<noid>/')
def objectdata(noid):
    assert re.match('^[a-z0-9]{12}$', noid)

    # Item or series?
    q ='''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dcterms: <http://purl.org/dc/terms/>

        ASK {{ <{0}> dcterms:isPartOf ?o }}
    '''.format(noid)

    r = requests.get(
        headers={
            'Content-type': 'application/sparql'
        },  
        url='http://marklogic.lib.uchicago.edu:8031/v1/graphs/sparql?query={}'.format(
            urllib.parse.quote_plus(q)
        )
    )   

    if r.text == 'true':
        is_item = True
    else:
        is_item = False

    if is_item:
        return jsonify(itemdata(noid))
    else:
        return jsonify(seriesdata(noid))

@app.route('/object/<noid>/')
def object(noid):
    assert re.match('^[a-z0-9]{12}$', noid)

    r = requests.get('{}/objectdata/{}/'.format(
        'https://mlp.lib.uchicago.edu',
        noid
    ))
    metadata = json.loads(r.text)

    # Item or series?
    is_item = any([m[0] == 'Part of Series' for m in metadata])
   
    if is_item:
        return item(noid, metadata)
    else:
        return series(noid, metadata)

def itemdata(noid):
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        CONSTRUCT {{
            <{0}> ?p ?o .
            <{0}> ?p1 ?o1 .
            ?o1 ?p2 ?o2 .
            <{0}> dc:title ?titleNode .
            ?titleNode dma:collectionTitle ?collectionTitle .
            ?titleNode dma:collectionTitleType ?collectionTitleType
        }} WHERE {{
            GRAPH <http://lib.uchicago.edu/mlp> {{
                <{0}> ?p ?o .
                <{0}> ?p1 ?o1 .
                ?o1 ?p2 ?o2 .
                <{0}> dcterms:isPartOf ?seriesNode .
                ?seriesNode dc:title ?titleNode .
                ?titleNode dma:collectionTitle ?collectionTitle .
                ?titleNode dma:collectionTitleType ?collectionTitleType
            }}
        }}
    '''.format(noid)

    r = requests.get(
        headers={
            'Accept': 'text/turtle',
            'Content-type': 'application/sparql'
        },  
        url='http://marklogic.lib.uchicago.edu:8031/v1/graphs/sparql?query={}'.format(
            urllib.parse.quote_plus(q)
        )
    )   

    graph = rdflib.Graph()

    for k, v in {
        'aes60-2011': 'https://www.aes.org/publications/standards/',
        'audioMD':    'http://www.loc.gov/audioMD',
        'bf':         'http://id.loc.gov/ontologies/bibframe/',
        'dc':         'http://purl.org/dc/elements/1.1/',
        'dcterms':    'http://purl.org/dc/terms/',
        'dma':        'http://lib.uchicago.edu/dma/',
        'edm':        'http://www.europeana.eu/schemas/edm/',
        'lexvo':      'https://www.iso.org/standard/39534.html',
        'olac':       'http://www.language−archives.org/OLAC/metadata.html',
        'ore':        'http://www.openarchives.org/ore/terms/',
        'vra':        'http://purl.org/vra/'
    }.items():
        graph.bind(k, rdflib.Namespace(v))

    graph.parse(data=r.text, format='turtle')

    # Panopto Identifier
    qres = graph.query('''
            BASE <https://ark.lib.uchicago.edu/ark:61001/>
            PREFIX dma: <http://lib.uchicago.edu/dma/>

            SELECT DISTINCT ?panoptoIdentifier
            WHERE {{ <{0}> dma:panoptoIdentifier ?panoptoIdentifier
            }} 
        '''.format(noid))
    panopto_identifiers = []
    for row in qres:
        panopto_identifiers.append(str(row[0]).strip())

    # Primary Titles
    qres = graph.query('''
            BASE <https://ark.lib.uchicago.edu/ark:61001/>

            PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
            PREFIX dma: <http://lib.uchicago.edu/dma/>

            SELECT DISTINCT ?itemTitle
            WHERE {{ <{}> bf:title ?titleNode .
                     ?titleNode dma:itemTitle ?itemTitle .
                     ?titleNode dma:itemTitleType ?itemTitleType .
                     FILTER(?itemTitleType = 'Primary')
            }} 
        '''.format(noid))
    primary_titles = []
    for row in qres:
        primary_titles.append(str(row[0]).strip())

    # Alternative Titles
    qres = graph.query('''
            BASE <https://ark.lib.uchicago.edu/ark:61001/>

            PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
            PREFIX dma: <http://lib.uchicago.edu/dma/>

            SELECT DISTINCT ?itemTitle
            WHERE {{ <{}> bf:title ?titleNode .
                     ?titleNode dma:itemTitle ?itemTitle .
                     ?titleNode dma:itemTitleType ?itemTitleType .
                     FILTER(?itemTitleType = 'Alternate')
            }} 
        '''.format(noid))
    alternative_titles = []
    for row in qres:
        alternative_titles.append(str(row[0]).strip())

    # Item Identifiers
    qres = graph.query('''
            BASE <https://ark.lib.uchicago.edu/ark:61001/>

            PREFIX dma: <http://lib.uchicago.edu/dma/>

            SELECT DISTINCT ?itemIdentifier
            WHERE {{ 
                <{0}> dma:itemIdentifier ?itemIdentifier
            }} 
        '''.format(noid))
    item_identifiers = []
    for row in qres:
        item_identifiers.append(str(row[0]).strip())

    # Creator
    qres = graph.query('''
            BASE <https://ark.lib.uchicago.edu/ark:61001/>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX dma: <http://lib.uchicago.edu/dma/>

            SELECT DISTINCT ?contributorName ?contributorRole ?contributorString
            WHERE {{
                <{0}> dc:contributor ?contributorNode .
                ?contributorNode dma:itemContributorName ?contributorName .
                ?contributorNode dma:itemContributorRole ?contributorRole . 
                ?contributorNode dma:itemContributorString ?contributorString
            }}
        '''.format(noid))
    creators = []
    for row in qres:
        creators.append({
            'name': str(row[0]).strip(),
            'role': str(row[1]).strip(),
            'string': str(row[2]).strip()
        })

    # Subject Languages
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>
        PREFIX lexvo: <https://www.iso.org/standard/39534.html>

        SELECT DISTINCT ?languageCode ?languageRole
        WHERE {{
            <{0}> dc:language ?languageNode .
            ?languageNode lexvo:iso639P3PCode ?languageCode .
            ?languageNode dma:languageRole ?languageRole .
            FILTER(?languageRole = 'Subject' || ?languageRole = 'Both')
        }}
    '''.format(noid))
    subject_languages = []
    for row in qres:
        label = glotto_labels(str(row[0]).strip())
        if label == '':
            label = str(row[0]).strip()
        subject_languages.append({
            'code': str(row[0]).strip(),
            'label': label,
            'role': str(row[1]).strip()
        })

    # Primary Languages
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>
        PREFIX lexvo: <https://www.iso.org/standard/39534.html>

        SELECT DISTINCT ?languageCode ?languageRole
        WHERE {{
            <{0}> dc:language ?languageNode .
            ?languageNode lexvo:iso639P3PCode ?languageCode .
            ?languageNode dma:languageRole ?languageRole .
            FILTER(?languageRole = 'Primary' || ?languageRole = 'Both')
        }}
    '''.format(noid))
    primary_languages = []
    for row in qres:
        label = glotto_labels(str(row[0]).strip())
        if label == '':
            label = str(row[0]).strip()
        primary_languages.append({
            'code': str(row[0]).strip(),
            'label': label,
            'role': str(row[1]).strip()
        })

    # Location of Recording
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?coverageIdentifier ?coverageType
        WHERE {{
            <{0}> dma:coverage ?coverageNode .
            ?coverageNode dcterms:spatial ?coverageIdentifier .
            ?coverageNode dma:itemCoverageType ?coverageType .
            FILTER(?coverageType = 'recording')
        }}
    '''.format(noid))
    location_of_recordings = []
    for row in qres:
        location_of_recordings.append({
            'identifier': str(row[0]).strip(),
            'label': str(row[0]).strip(),
            'type': str(row[1]).strip()
        })

    # Country of Language
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?coverageIdentifier ?coverageType
        WHERE {{
            <{0}> dma:coverage ?coverageNode .
            ?coverageNode dcterms:spatial ?coverageIdentifier .
            ?coverageNode dma:itemCoverageType ?coverageType .
            FILTER(?coverageType = 'language')
        }}
    '''.format(noid))
    country_of_languages = []
    for row in qres:
        country_of_languages.append({
            'identifier': str(row[0]).strip(),
            'label': str(row[0]).strip(),
            'type': str(row[1]).strip()
        })

    # Date
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?displayDate
        WHERE {{
            <{0}> dma:displayDate ?displayDate 
        }}
    '''.format(noid))
    dates = []
    for row in qres:
        dates.append(str(row[0]).strip())

    # Description
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dc: <http://purl.org/dc/elements/1.1/>

        SELECT DISTINCT ?description
        WHERE {{
            <{0}> dc:description ?description
        }}
    '''.format(noid))
    descriptions = []
    for row in qres:
        descriptions.append(str(row[0]).strip())

    # Linguistic Data Types
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?linguisticDataType
        WHERE {{
            <{0}> dma:linguisticDataType ?linguisticDataType
        }}
    '''.format(noid))
    linguistic_data_types = []
    for row in qres:
        linguistic_data_types.append(str(row[0]).strip())

    # Discourse Types
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX olac: <http://www.language−archives.org/OLAC/metadata.html>

        SELECT DISTINCT ?discourseType
        WHERE {{
            <{0}> olac:discourseType ?discourseType
        }}
    '''.format(noid))
    discourse_types = []
    for row in qres:
        discourse_types.append(str(row[0]).strip())

   # Item Content Type
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?itemContentType
        WHERE {{
            <{0}> dma:DMAContentType ?itemContentType
        }}
    '''.format(noid))
    item_content_types = []
    for row in qres:
        item_content_types.append(str(row[0]).strip())

   # Part of Series
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?seriesNode ?seriesTitle
        WHERE {{
            <{0}> dcterms:isPartOf ?seriesNode .
            ?seriesNode dc:title ?titleNode .
            ?titleNode dma:collectionTitle ?seriesTitle .
            ?titleNode dma:collectionTitleType ?seriesTitleType .
            FILTER(?seriesTitleType = 'Primary')
        }}
    '''.format(noid))
    part_of_series = []
    for row in qres:
        series_identifier = str(row[0]).strip().replace('https://ark.lib.uchicago.edu/ark:61001/', '')
        series_title = str(row[1]).strip()
        part_of_series.append({
            'identifier': series_identifier,
            'title': series_title
        })

    # Access Level
    qres = graph.query('''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT DISTINCT ?accessRights
        WHERE {{
            <{0}> dcterms:isPartOf ?seriesNode .
            ?seriesNode dcterms:accessRights ?accessRights
        }}
    '''.format(noid))
    access_levels = []
    for row in qres:
        access_levels.append(str(row[0]).strip())

    metadata = []
    if panopto_identifiers:
        metadata.append(('Panopto Identifer', panopto_identifiers))
    if primary_titles:
        metadata.append(('Item Title', primary_titles))
    if alternative_titles:
        metadata.append(('Alternative Title', alternative_titles))
    if item_identifiers:
        metadata.append(('Item Identifier', item_identifiers))
    if creators:
        metadata.append(('Contributor', creators))
    if subject_languages: 
        metadata.append(('Subject Language', subject_languages))
    if primary_languages:
        metadata.append(('Primary Language', primary_languages))
    if location_of_recordings:
        metadata.append(('Location of Language', location_of_recordings))
    if country_of_languages:
        metadata.append(('Country of Language', country_of_languages))
    if dates:
        metadata.append(('Date', dates))
    if descriptions:
        metadata.append(('Description', descriptions))
    if linguistic_data_types:
        metadata.append(('Linguistic Data Type', linguistic_data_types))
    if discourse_types:
        metadata.append(('Discourse Type', discourse_types))
    if item_content_types:
        metadata.append(('Item Content Type', item_content_types))
    if part_of_series:
        metadata.append(('Part of Series', part_of_series))
    return metadata

def item(noid, metadata):
    panopto_identifier = ''
    rights = ''
    title = ''
    metadata_out = []
    for m in metadata:
        if m[0] == 'Panopto Identifier':
            panopto_identifier = m[1][0]
        else:
            if m[0] == 'Item Title':
                title = m[1][0]
            metadata_out.append(m)

    return render_template(
        'item.html',
        metadata = metadata_out,
        panopto_identifier = panopto_identifier,
        rights = rights,
        title = title
    )

def seriesdata(noid):
    '''Pre-process series data. Move contributors, coverages, dates and
       languages up from item nodes into series nodes. Include all item
       nodes belonging to the series, but prune each to include only
       the title, identifier, and isPartOf. 

        Args: 
          noid - a string, the NOID for this series.
 
        Returns:
          an array of JSON-LD data.
    '''

    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        CONSTRUCT {{
            <{0}> dcterms:accessRights ?accessRightsSubject .
            <{0}> dc:contributor ?contributorNode .
            ?contributorNode ?contributorPredicate ?contributorSubject .
            <{0}> dc:description ?descriptionSubject .
            <{0}> dc:identifier ?identifierSubject .
            <{0}> dc:language ?languageNode .
            ?languageNode ?languagePredicate ?languageSubject .
            <{0}> dc:title ?titleNode .
            ?titleNode ?titlePredicate ?titleSubject .
            <{0}> dma:coverage ?coverageNode .
            ?coverageNode ?coveragePredicate ?coverageSubject .
            <{0}> dma:displayDate ?dateSubject .
            ?itemNode bf:title ?itemTitleNode .
            ?itemNode dcterms:isPartOf <{0}> .
            ?itemNode dma:itemIdentifier ?itemIdentifier .
            ?itemTitleNode ?itemTitlePredicate ?itemTitleObject .
            <{0}> rdf:type ?typeSubject
        }} WHERE {{
            {{
                <{0}> dcterms:accessRights ?accessRightsSubject
            }} UNION {{
                ?contributorItem dcterms:isPartOf <{0}> .
                ?contributorItem dc:contributor ?contributorNode .
                ?contributorNode ?contributorPredicate ?contributorSubject .
                ?contributorNode dma:itemContributorRole ?contributorRole .
                FILTER(?contributorRole != 'contributor')
            }} UNION {{
                <{0}> dma:coverage ?coverageNode .
                ?coverageNode ?coveragePredicate ?coverageSubject .
            }} UNION {{
                ?dateItem dcterms:isPartOf <{0}> .
                ?dateItem dma:displayDate ?dateSubject
            }} UNION {{
                <{0}> dc:description ?descriptionSubject 
            }} UNION {{
                <{0}> dc:identifier ?identifierSubject
            }} UNION {{
                <{0}> dc:title ?titleNode .
                ?titleNode ?titlePredicate ?titleSubject
            }} UNION {{
                ?itemNode dcterms:isPartOf <{0}> .
                ?itemNode dma:itemIdentifier ?itemIdentifier
            }} UNION {{
                ?itemNode dcterms:isPartOf <{0}> .
                ?itemNode bf:title ?itemTitleNode .
                ?itemTitleNode ?itemTitlePredicate ?itemTitleObject
            }} UNION {{
                ?languageItem dcterms:isPartOf <{0}> .
                ?languageItem dc:language ?languageNode .
                ?languageNode ?languagePredicate ?languageSubject
            }} UNION {{
                <{0}> rdf:type ?typeSubject 
            }}
        }}
    '''.format(noid)

    r = requests.get(
        headers={
            'Accept': 'text/turtle',
            'Content-type': 'application/sparql'
        },  
        url='http://marklogic.lib.uchicago.edu:8031/v1/graphs/sparql?query={}'.format(
            urllib.parse.quote_plus(q)
        )
    )   

    subgraph = rdflib.Graph()
    for k, v in {
        'aes60-2011': 'https://www.aes.org/publications/standards/',
        'audioMD':    'http://www.loc.gov/audioMD',
        'bf':         'http://id.loc.gov/ontologies/bibframe/',
        'dc':         'http://purl.org/dc/elements/1.1/',
        'dcterms':    'http://purl.org/dc/terms/',
        'dma':        'http://lib.uchicago.edu/dma/',
        'edm':        'http://www.europeana.eu/schemas/edm/',
        'lexvo':      'https://www.iso.org/standard/39534.html',
        'olac':       'http://www.language−archives.org/OLAC/metadata.html',
        'ore':        'http://www.openarchives.org/ore/terms/',
        'vra':        'http://purl.org/vra/'
    }.items():
        subgraph.bind(k, rdflib.Namespace(v))

    subgraph.parse(data=r.text, format='turtle')

    #############
    # STAGE TWO #
    #############

    # Primary Titles
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?primaryTitle
        WHERE {{
            <{0}> dc:title ?titleNode .
            ?titleNode dma:collectionTitle ?primaryTitle .
            ?titleNode dma:collectionTitleType ?collectionTitleType .
            FILTER(?collectionTitleType = 'Primary')
        }}
    '''.format(noid)

    primary_titles = []
    qres = subgraph.query(q)
    for row in qres:
        primary_titles.append(str(row[0]).strip())

    # Alternative Titles
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?alternativeTitle
        WHERE {{
            <{0}> dc:title ?titleNode .
            ?titleNode dma:collectionTitle ?alternativeTitle .
            ?titleNode dma:collectionTitleType ?collectionTitleType .
            FILTER(?collectionTitleType = 'Alternate')
        }}
    '''.format(noid)

    alternative_titles = []
    qres = subgraph.query(q)
    for row in qres:
        alternative_titles.append(str(row[0]).strip())

    # Series Identifier
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>

        SELECT DISTINCT ?seriesIdentifier
        WHERE {{
            <{0}> dc:identifier ?seriesIdentifier  .
        }}
    '''.format(noid)

    series_identifiers = []
    qres = subgraph.query(q)
    for row in qres:
        series_identifiers.append(str(row[0]).strip())

    # Collections
    collections = ['Digital Media Archive']

    # Creator
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?contributorName ?contributorRole ?contributorString
        WHERE {{
            <{0}> dc:contributor ?contributorNode .
            ?contributorNode dma:itemContributorName ?contributorName .
            ?contributorNode dma:itemContributorRole ?contributorRole . 
            ?contributorNode dma:itemContributorRole ?contributorString
        }}
    '''.format(noid)

    creators = []
    qres = subgraph.query(q)
    for row in qres:
        creators.append({
            'name': str(row[0]).strip(),
            'role': str(row[1]).strip(),
            'string': str(row[2]).strip()
        })

    # Subject Languages
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>
        PREFIX lexvo: <https://www.iso.org/standard/39534.html>

        SELECT DISTINCT ?languageCode ?languageRole
        WHERE {{
            <{0}> dc:language ?languageNode .
            ?languageNode lexvo:iso639P3PCode ?languageCode .
            ?languageNode dma:languageRole ?languageRole .
            FILTER(?languageRole = 'Subject' || ?languageRole = 'Both')
        }}
    '''.format(noid)
    subject_languages = []
    qres = subgraph.query(q)
    for row in qres:
        label = glotto_labels(str(row[0]).strip())
        if label == '':
            label = str(row[0]).strip()
        subject_languages.append({
            'code': str(row[0]).strip(),
            'label': label,
            'role': str(row[1]).strip()
        })

    # Primary Languages
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>
        PREFIX lexvo: <https://www.iso.org/standard/39534.html>

        SELECT DISTINCT ?languageCode ?languageRole
        WHERE {{
            <{0}> dc:language ?languageNode .
            ?languageNode lexvo:iso639P3PCode ?languageCode .
            ?languageNode dma:languageRole ?languageRole .
            FILTER(?languageRole = 'Primary' || ?languageRole = 'Both')
        }}
    '''.format(noid)
    primary_languages = []
    qres = subgraph.query(q)
    for row in qres:
        label = glotto_labels(str(row[0]).strip())
        if label == '':
            label = str(row[0]).strip()
        primary_languages.append({
            'code': str(row[0]).strip(),
            'label': label,
            'role': str(row[1]).strip()
        })

    # Location of Recording
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?coverageIdentifier ?coverageType
        WHERE {{
            <{0}> dma:coverage ?coverageNode .
            ?coverageNode dcterms:spatial ?coverageIdentifier .
            ?coverageNode dma:itemCoverageType ?coverageType .
            FILTER(?coverageType = 'recording')
        }}
    '''.format(noid)
    location_of_recordings = []
    qres = subgraph.query(q)
    for row in qres:
        location_of_recordings.append({
            'identifier': str(row[0]).strip(),
            'label': str(row[0]).strip(),
            'type': str(row[1]).strip()
        })

    # Country of Language
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?coverageIdentifier ?coverageType
        WHERE {{
            <{0}> dma:coverage ?coverageNode .
            ?coverageNode dcterms:spatial ?coverageIdentifier .
            ?coverageNode dma:itemCoverageType ?coverageType .
            FILTER(?coverageType = 'language')
        }}
    '''.format(noid)
    country_of_languages = []
    qres = subgraph.query(q)
    for row in qres:
        country_of_languages.append({
            'identifier': str(row[0]).strip(),
            'label': str(row[0]).strip(),
            'type': str(row[1]).strip()
        })

    # Date
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?displayDate
        WHERE {{
            <{0}> dma:displayDate ?displayDate 
        }}
    '''.format(noid)
    dates = []
    qres = subgraph.query(q)
    for row in qres:
        dates.append(str(row[0]).strip())

    # Description
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX dc: <http://purl.org/dc/elements/1.1/>

        SELECT DISTINCT ?description
        WHERE {{
            <{0}> dc:description ?description
        }}
    '''.format(noid)
    descriptions = []
    qres = subgraph.query(q)
    for row in qres:
        descriptions.append(str(row[0]).strip())

    # Items
    q = '''
        BASE <https://ark.lib.uchicago.edu/ark:61001/>

        PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dma: <http://lib.uchicago.edu/dma/>

        SELECT DISTINCT ?itemNode ?itemIdentifier ?itemTitle
        WHERE {{
            ?itemNode dcterms:isPartOf <{0}> .
            ?itemNode dma:itemIdentifier ?itemIdentifier .
            ?itemNode bf:title ?itemTitleNode .
            ?itemTitleNode dma:itemTitle ?itemTitle
        }}
    '''.format(noid)
    items = []
    qres = subgraph.query(q)
    for row in qres:
        items.append({
            'identifier': str(row[1]).strip(),
            'noid': str(row[0]).strip().split('/').pop(),
            'title': str(row[2].strip())
        })

    metadata = []
    if primary_titles:
        metadata.append(('Primary Title', primary_titles))
    if alternative_titles:
        metadata.append(('Alternative Title', alternative_titles))
    if series_identifiers:
        metadata.append(('Series Identifier', series_identifiers))
    if collections:
        metadata.append(('Collection', collections))
    if creators:
        metadata.append(('Creator', creators))
    if subject_languages:
        metadata.append(('Subject Language', subject_languages))
    if primary_languages:
        metadata.append(('Primary Language', primary_languages))
    if location_of_recordings: 
        metadata.append(('Location of Recording', location_of_recordings))
    if country_of_languages:
        metadata.append(('Country of Language', country_of_languages))
    if dates:
        metadata.append(('Date', dates))
    if descriptions:
        metadata.append(('Description', descriptions))
    if items:
        metadata.append(('Items', items))
    return metadata

def series(noid, metadata):
    # extract primary title and items.
    metadata_out = []
    title = []
    items = []
    for m in metadata:
        if m[0] == 'Primary Title':
            title = m[1][0]
        elif m[0] == 'Items':
            items = m[1]
        else:
            metadata_out.append(m)

    return render_template(
        'series.html',
        items = items,
        metadata = metadata_out,
        rights = 'test',
        title = title
    )

@app.route('/search/')
def search():
    query = request.args.get('query')
    collection = request.args.get('collection')
    language = request.args.get('language')
    spatial = request.args.get('spatial')
    format = 'xml'

    assert query in ('language', 'spatial')
    assert collection in ('mila',)

    params = {
        'collection': collection,
        'format': format,
        'query': query
    }

    if query == 'language':
        params['language'] = language
        url = MARKLOGIC_SERVER + '/chas_query.xqy?' + urllib.parse.urlencode(params)
        r = requests.get(url)
        xml = etree.fromstring(r.text)
        results = process_search_results(xml)
    if query == 'spatial':
        params['spatial'] = spatial
        url = MARKLOGIC_SERVER + '/chas_query.xqy?' + urllib.parse.urlencode(params)
        r = requests.get(url)
        xml = etree.fromstring(r.text)
        results = process_search_results(xml)

    return render_template(
        'search.html',
        facets = results['facets'],
        results = results['results']
    )
    
