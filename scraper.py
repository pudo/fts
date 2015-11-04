#coding: utf-8
import os
import urllib
import logging
import zipfile
import HTMLParser
import string
from datetime import datetime

import requests
import dataset
from lxml import etree

log = logging.getLogger('fts')

CACHE_DIR = 'data'
NUMCHAR = "0123456789-."
BASE_URL = 'http://ec.europa.eu/budget/remote/fts/dl/export_%s_en.zip'

engine = dataset.connect('sqlite:///data.sqlite')
entry = engine['data']


def to_float(num):
    try:
        num = num.replace('.', '').replace(',', '.')
        return float(''.join([n for n in num if n in NUMCHAR]))
    except:
        "NaN"


def convert_commitment(base, commitment):
    common = {}
    common['date'] = commitment.findtext('year')
    common['total'] = to_float(commitment.findtext('amount'))
    common['cofinancing_rate'] = commitment.findtext('cofinancing_rate')
    common['cofinancing_rate_pct'] = to_float(common['cofinancing_rate'])
    common['position_key'] = commitment.findtext('position_key')
    common['grant_subject'] = commitment.findtext('grant_subject')
    common['responsible_department'] = commitment.findtext('responsible_department')
    common['action_type'] = commitment.findtext('actiontype')
    budget_line = commitment.findtext('budget_line')

    name, code = budget_line.rsplit('(', 1)
    code = code.replace(')', '').replace('"', '').strip()
    common['budget_item'] = name.strip()
    common['budget_code'] = code

    parts = code.split(".")
    common['title'] = parts[0]
    common['chapter'] = '.'.join(parts[:2])
    common['article'] = '.'.join(parts[:3])
    if len(parts) == 4:
        common['item'] = '.'.join(parts[:4])

    for beneficiary in commitment.findall('.//beneficiary'):
        row = common.copy()
        row['beneficiary'] = beneficiary.findtext('name')
        if '*' in row['beneficiary']:
            row['beneficiary'], row['alias'] = row['beneficiary'].split('*', 1)
        else:
            row['alias'] = row['beneficiary']
        row['address'] = beneficiary.findtext('address')
        row['vat_number'] = beneficiary.findtext('vat')
        row['expensetype'] = beneficiary.findtext('expensetype')
        row['city'] = beneficiary.findtext('city')
        row['postcode'] = beneficiary.findtext('post_code')
        row['country'] = beneficiary.findtext('country')
        row['geozone'] = beneficiary.findtext('geozone')
        row['coordinator'] = beneficiary.findtext('coordinator')
        detail_amount = beneficiary.findtext('detail_amount')
        if detail_amount is not None and len(detail_amount):
            row['amount'] = to_float(detail_amount)
        else:
            row['amount'] = row['total']
        if row['amount'] is "NaN":
            row['amount'] = row['total']

        base['source_id'] += 1
        row.update(base)
        log.info('%s - %s', row['grant_subject'], row['beneficiary'])
        entry.upsert(row, ['source_file', 'source_id'])


def clean_text(text):
    h = HTMLParser.HTMLParser()
    text = filter(string.printable.__contains__, text)
    text = h.unescape(text)
    # text = text.replace('<![CDATA[', '')
    text = text.replace(']]</', ']]></')
    text = text.replace('&', '&amp;')
    return text.encode('utf-8')


def convert_file(fh, url):
    text = fh.read().decode('utf-8')
    text = clean_text(text)
    doc = etree.fromstring(text)
    base = {'source_url': url, 'source_id': 0}
    for i, commitment in enumerate(doc.findall('.//commitment')):
        base['source_line'] = commitment.sourceline
        base['source_contract_id'] = i
        convert_commitment(base, commitment)


def download():
    try:
        os.makedirs(CACHE_DIR)
    except:
        pass
    for year in range(2012, datetime.now().year):
        log.info("Downloading FTS for %s", year)
        url = BASE_URL % year
        fn = os.path.join(CACHE_DIR, 'export_%s.zip' % year)
        urllib.urlretrieve(url, fn)
        with zipfile.ZipFile(fn, 'r') as zf:
            for name in zf.namelist():
                fh = zf.open(name, 'r')
                convert_file(fh, url)
                # for evt, el in etree.iterparse(fh):
                #     pass
                #     # print evt, el


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    download()
