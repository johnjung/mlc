import json, os, re, requests, urllib
from flask import Flask, render_template, request
from lxml import etree as etree

app = Flask(__name__)

MARKLOGIC_SERVER = os.environ['MARKLOGIC_SERVER']
PROXY_SERVER = os.environ['PROXY_SERVER']

OBJECT_PAGE_SORTED_LABELS = (
  ('http://purl.org/dc/elements/1.1/title',                 'Title'),
  ('http://purl.org/dc/elements/1.1/description',           'Description'),
  ('http://lib.uchicago.edu/ucla/invertedLanguageName',     'Language'),
  ('http://purl.org/dc/terms/spatial',                      'Location'),
  ('http://purl.org/dc/elements/1.1/contributor',           'Contributor'),
  ('http://purl.org/dc/elements/1.1/creator',               'Creator'),
  ('http://purl.org/dc/elements/1.1/type',                  'Type'),
  ('http://purl.org/dc/terms/identifier',                   'TermsIdentifier'),
  ('http://purl.org/dc/terms/rights',                       'Rights'),
  ('http://lib.uchicago.edu/ucla/contributorStringDisplay', 'ContributorStringDisplay'),
)

if __name__ == '__main__':
    app.run(host='0.0.0.0')

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

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

    '''
    {
      "ark:61001/b2zq00v3fg6z": {
        "http://lib.uchicago.edu/ucla/invertedLanguageName": [
          "Castilian",
          "Quich\u00e9, Central",
          "Spanish"
        ],
        "http://purl.org/dc/terms/rights": [
          "Public domain"
        ]
      },
      ...
    }
    '''

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

@app.route('/object/<noid>/')
def object(noid):
    assert re.match('^[a-z0-9]{12}$', noid)

    url = '{}/chas_query.xqy?query=identifier&collection=mila&identifier={}&format=xml'.format(
        MARKLOGIC_SERVER,
        noid
    )
    r = requests.get(url)

    xml = etree.fromstring(r.text)

    # build a dictionary where keys are the predicate URI and values are an
    # array of object literals for each predicate. E.g.,
    # {
    #   'http://id.loc.gov/ontologies/bibframe/title': ['HanksF152'],
    #   ...
    # }
    result_dict = {}
    for result in xml.xpath(
        '//sparql:result',
        namespaces={'sparql': 'http://www.w3.org/2005/sparql-results#'}
    ):
        p = result.xpath(
          'sparql:binding[@name="p"]/sparql:uri', 
          namespaces={'sparql': 'http://www.w3.org/2005/sparql-results#'}
        )[0].text
        o = result.xpath(
          'sparql:binding[@name="o"]/sparql:literal', 
          namespaces={'sparql': 'http://www.w3.org/2005/sparql-results#'}
        )[0].text
        if p not in result_dict:
            result_dict[p] = []
        result_dict[p].append(o)

    metadata = [] 
    for p, label in OBJECT_PAGE_SORTED_LABELS:
        if p in result_dict:
            metadata.append((label, result_dict[p]))

    title = ''
    if 'http://purl.org/dc/elements/1.1/title' in result_dict:
        title = result_dict['http://id.loc.gov/ontologies/bibframe/title'][0]

    rights = 'Restricted'
    if 'http://purl.org/dc/terms/rights' in result_dict:
        rights = result_dict['http://purl.org/dc/terms/rights'][0]

    return render_template(
        'object.html',
        metadata = metadata,
        rights = rights,
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

    print(json.dumps(results['facets'], cls=SetEncoder, indent=2))

    return render_template(
        'search.html',
        facets = results['facets'],
        results = results['results']
    )
    
