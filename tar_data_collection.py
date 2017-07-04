#coding=utf-8

"""
Data collection script for TAR :: CLEF eHealth 2017 Task 2: Technologically Assisted Reviews in Empirical Medicine
https://sites.google.com/site/clefehealth2017/task-2

Author: Dan Li (d.li@uva.nl)
==================================================================================================================
Requirement
----------
1. Download chromedriver from https://sites.google.com/a/chromium.org/chromedriver/
2. Install selenium: pip install selenium

Functions
----------
 batch_download_pid           --- Download pids for all the systematic reviews
 extract_pid                  --- Extract pids from downloaded xml and rewrite to new dir
 batch_download_title         --- Download title for all the systematic reviews
 make_release_file            --- Make release files: topic file or qrel file
 download_abstract            --- Download abstract for all the pids
 trec_format_abstract         --- Make the downloaded abstracts TRECTEXT format
 statistics                   --- Statistics of the released data

"""


import os
import re
import csv
import math
import time
import codecs
import datetime
import requests
import pandas as pd
from operator import itemgetter
from collections import defaultdict

import xml.dom.minidom
from time import sleep
from openpyxl import load_workbook

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException, TimeoutException


# Directories
BASE_DIR = os.path.dirname(os.path.realpath(__file__))

CHROMEDRIVER_DIR = os.path.join(BASE_DIR, 'chromedriver')
DOWNLOAD_PIDS_DIR = os.path.join(BASE_DIR, 'download_pids')
PIDS_DIR = os.path.join(BASE_DIR, 'pids')
TITLE_DIR = os.path.join(BASE_DIR, 'title.txt')
TOPIC_DIR = os.path.join(BASE_DIR, 'topic')
DOC_QREL_DIR = os.path.join(BASE_DIR, 'doc_qrel')
ABS_QREL_DIR = os.path.join(BASE_DIR, 'abs_qrel')
CORPORA_DIR = os.path.join(BASE_DIR, 'copora')
TRECTEXT_DIR = os.path.join(BASE_DIR, 'trectext')

OVID_URL = "http://demo.ovid.com/demo/ovidsptools/launcher.htm"
NCBI_API_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
OVID_SEARCH_FILE = 'medline_ovid_search.xlsx'
RELEVANCE_INDEX_FILE = 'relevance_index.csv'

DOWNLOAD_NUM_PER_TIME = 500  # document num
IMPLICIT_WAIT_TIME = 60  # second
EXPLICIT_WAIT_TIME = 120  # second
EXPLICIT_WAIT_INTERVAL = 2  # second


def check_existing():
    """
    Check existing of directories
    :return:
    """
    for mdir in [DOWNLOAD_PIDS_DIR, PIDS_DIR, TOPIC_DIR, DOC_QREL_DIR, ABS_QREL_DIR, CORPORA_DIR, TRECTEXT_DIR]:
        if not os.path.exists(mdir):
            os.makedirs(mdir)
    return


def chunks_by_element(arr, n):
    """
    Example:
    chunks_by_element(range(10),3)
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    :param arr:
    :param n:
    :return:
    """
    return [arr[i:i+n] for i in range(0, len(arr), n)]


def chunks_by_piece(arr, m):
    """
    Example:
    chunks_by_piece(range(10),3)
    [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]

    :param arr:
    :param m:
    :return:
    """
    n = int(math.ceil(len(arr) / float(m)))
    return [arr[i:i + n] for i in range(0, len(arr), n)]


def get_file_ids(path):
    """
    Get all file names in the directory
    :param path:
    :return:
    """
    file_ids = []
    for root, dirs, files in os.walk(path):
        file_ids.extend(files)
    file_ids = [f for f in file_ids if not f.startswith('.')]

    return file_ids


def get_dirs(path):
    """
    Get all the directories in the directory
    :param path:
    :return:
    """
    list_dirs = []
    for root, dirs, files in os.walk(path):
        list_dirs.extend(dirs)
    return list_dirs


def get_tag_text(root, tagname):
    """
    Get text by tagname
    :param root:
    :param tagname:
    :return:
    """
    node = root.getElementsByTagName(tagname)[0]
    rc = ''
    for node in node.childNodes:
        if node.TEXT_NODE == node.nodeType:
            rc = rc + node.data
    return rc


def get_tag_list(root, tagname):
    """
    Get all the elements under root by tagname
    :param root:
    :param tagname:
    :return:
    """
    node = root.getElementsByTagName(tagname)[0]
    lst = []
    for n in node.childNodes:
        if n.ELEMENT_NODE == n.nodeType:
            if [] == n.childNodes:
                lst.append('')
            else:
                if n.TEXT_NODE == n.childNodes[0].nodeType:
                    lst.append(n.childNodes[0].data)

    return lst


def read_ovid_search_file():
    """
    Read medline_ovid_search.xlsx made by Rene (medical expert).
    :return: dict
    """
    dict_review = defaultdict(dict)

    # Load *.xlsx file
    wb = load_workbook(os.path.join(BASE_DIR, OVID_SEARCH_FILE))
    sheet_ranges = wb['Sheet1']

    for i in range(2, 53):  # Line 1-52 are valid lines
        ori_query = sheet_ranges['D{}'.format(i)].value.strip()
        list_ori_query = [line.strip() for line in ori_query.split('\n') if line.strip() != '']

        query = '\n'.join(list_ori_query)
        date_limit = sheet_ranges['F{}'.format(i)].value
        link = sheet_ranges['B{}'.format(i)].value
        topic_id = int(sheet_ranges['A{}'.format(i)].value)

        topic_id = str(topic_id)
        dict_review[topic_id]['review_doi'] = re.findall(r'CD\d+', link)[0].strip()
        dict_review[topic_id]['url'] = link.strip()
        dict_review[topic_id]['query'] = query.strip()
        dict_review[topic_id]['date'] = date_limit.strip()

    print('dict_review keys length: {} \ndict_review keys: {} \n'.format(len(dict_review.keys()), dict_review.keys()))

    return dict_review


def read_clef_rel(qrel_type):
    """
    Read relevance judgement file made by Rene (medical expert).
    :param qrel_type: str, 'abs' or 'doc'
    :return: dict
    """
    dict_rel = defaultdict(dict)
    with codecs.open(os.path.join(BASE_DIR, RELEVANCE_INDEX_FILE), 'r', 'utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            review_doi = re.findall(r'CD\d+', row['review_doi'])[0].strip()
            pubmed_id = row['pubmed_id'].strip()
            # abs
            if 'abs' == qrel_type:
                if 'included' == row['ref_type']:
                    ref_type = 1
                elif 'excluded' == row['ref_type']:
                    ref_type = 1
                else:
                    continue

            # doc
            elif 'doc' == qrel_type:
                if 'included' == row['ref_type']:
                    ref_type = 1
                elif 'excluded' == row['ref_type']:
                    ref_type = 0
                else:
                    continue
            else:
                break
            dict_rel[review_doi][pubmed_id] = ref_type
    return dict_rel


def record_log(topic_id, search_query, err_msg):
    """
    Record error during downloading.
    :param topic_id:
    :param search_query:
    :param err_msg:
    :return:
    """
    with codecs.open(os.path.join(BASE_DIR, 'log.txt'), 'a', encoding='utf-8') as f:
        f.write('{}\n\n\n'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))

        f.write('topic id: {} \n\n'.format(topic_id))
        f.write('search query:\n {} \n\n'.decode('utf-8').format(search_query))
        f.write('error msg: \n {} \n\n'.decode('utf-8').format(err_msg))
    return


def download_pid_by_topic_id(topic_id, str_search_query):
    """
    Download pids by topic id
    :param topic_id:
    :param str_search_query:
    :return:
    """
    print('processing systematic review {}'.format(topic_id))

    # remove all the blanks at the beginning of every line
    str_search_query = '\n'.join(line.strip() for line in str_search_query.split('\n'))

    # set download directory
    topic_dir = os.path.join(DOWNLOAD_PIDS_DIR, str(topic_id))
    if not os.path.exists(topic_dir):  # make dir
        os.makedirs(topic_dir)

    # chrome settings
    chromeptions = webdriver.ChromeOptions()
    prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': topic_dir}
    chromeptions.add_experimental_option('prefs', prefs)

    # start chrome browser
    chromedriver = CHROMEDRIVER_DIR
    os.environ["webdriver.chrome.driver"] = chromedriver
    driver = webdriver.Chrome(executable_path=chromedriver, chrome_options=chromeptions)

    # request and response of website
    driver.get(OVID_URL)

    # set mesz
    driver.find_element_by_name('D').clear()
    driver.find_element_by_name('D').send_keys('mesz')

    # set search command line
    driver.find_element_by_name('SEARCH').clear()
    driver.find_element_by_name('SEARCH').send_keys(str_search_query)

    # submit
    driver.find_element_by_name('ovid').click()

    driver.implicitly_wait(IMPLICIT_WAIT_TIME)

    # jump to new window
    current_window_handle = driver.current_window_handle  # There seems to be only two handles: current and new
    for hdl in driver.window_handles:
        if hdl != current_window_handle:
            new_window_handle = hdl
    driver.switch_to.window(new_window_handle)

    try:

        # get returned document num
        try:
            # WARNING: must wait until webpage loading is finished, then get search_ret_num
            search_ret_num = WebDriverWait(driver, EXPLICIT_WAIT_TIME, EXPLICIT_WAIT_INTERVAL).until(
                ec.presence_of_element_located((By.XPATH, '//*[@id="searchaid-numbers"]')))
            search_ret_num = int(re.findall(r'\d+', search_ret_num.text.encode('utf-8'))[0])  #  Extract from str like "30963 text results"
        except TimeoutException:
            record_log(topic_id=topic_id, search_query=str_search_query, err_msg='system timeout.')
            return

        try:
            #  grammatical error exists in search query, record error for medical expert to analyze
            error = driver.find_element_by_xpath('//*[@id="msp-error-easy"]')
            record_log(topic_id=topic_id, search_query=str_search_query, err_msg=error.text)
            return
        except NoSuchElementException:
            # if webpage loading timeout happens, just pass.
            pass

        # OVID system allows to download maximum 500 documents per time
        list_range = chunks_by_element(range(1, search_ret_num+1), DOWNLOAD_NUM_PER_TIME)

        for item in list_range:

            # input range, e.g. 1-500
            download_range = driver.find_element_by_xpath('//*[@id="titles-display"]//input[@title="Range"]')
            download_range.clear()
            download_range.send_keys('{}-{}'.format(item[0], item[-1]))

            # click Export
            export = driver.find_element_by_xpath('//*[@id="titles-display"]//input[@value="Export"]')
            export.click()

            # get export-citation-popup window
            driver.switch_to.alert
            WebDriverWait(driver, 10, EXPLICIT_WAIT_INTERVAL).until(
                ec.presence_of_element_located((By.XPATH, '//div[@id="export-citation-popup"]')))

            # set file format option
            export_to_options = driver.find_element_by_xpath('//select[@id="export-citation-export-to-options"]')
            export_to_options.find_element_by_xpath('//option[@value="xml"]').click()  # XML

            # set citation content radio
            citation_options = driver.find_element_by_xpath('//ul[@id="export-citation-options"]')
            citation_options.find_element_by_xpath('//input[@value="SUBJECT"]').click()  # Part Reference

            # set include check-box
            citation_include = driver.find_element_by_xpath('//div[@id="export-citation-include"]')

            if citation_include.find_element_by_xpath('//input[@name="externalResolverLink"]').is_selected():  # Link to External Resolver
                citation_include.find_element_by_xpath('//input[@name="externalResolverLink"]').click()

            if citation_include.find_element_by_xpath('//input[@name="jumpstartLink"]').is_selected():  # Include URL
                citation_include.find_element_by_xpath('//input[@name="jumpstartLink"]').click()

            if citation_include.find_element_by_xpath('//input[@name="saveStrategy"]').is_selected():  # Search History
                citation_include.find_element_by_xpath('//input[@name="saveStrategy"]').click()

            # download
            download = driver.find_element_by_xpath('//div[@class ="export-citation-buttons"]')
            download.click()

    finally:
        sleep(30)  # wait for finishing downloading the last file
        # driver.implicitly_wait(30)  # doesn't work!
        driver.quit()

    return


def batch_download_pid():
    """
    Download pids for all systematic reviews
    :return:
    """
    # read clef reviews
    dict_review = read_ovid_search_file()

    # download pids
    for topic_id in dict_review.keys():
        download_pid_by_topic_id(topic_id, dict_review[topic_id]['query'])

    return


def extract_pid():
    """
    Extract pids from downloaded xml, filter out those that do not satisfy date constraint,
    and rewrite the left pids to new dir.
    :return:
    """

    # get year constraints
    dict_review = read_ovid_search_file()

    for topic_id in get_dirs(DOWNLOAD_PIDS_DIR):
        # read download pids data
        list_ret = []
        for mfile in get_file_ids(os.path.join(DOWNLOAD_PIDS_DIR, topic_id)):
            print('processing topic {} file {}'.format(topic_id, mfile))

            # date range
            date_range = dict_review[topic_id]['date']
            start_date, end_date = re.findall(r'\d+', date_range)
            start_date = datetime.datetime.strptime(start_date, "%Y%m%d")  # e.g. 20171230
            end_date = datetime.datetime.strptime(end_date, "%Y%m%d")

            # open xml
            dom = xml.dom.minidom.parse(os.path.join(DOWNLOAD_PIDS_DIR, topic_id, mfile))

            # get root elements
            root = dom.documentElement

            for r in root.getElementsByTagName('record'):
                inx = 0
                ui = ''
                test_date = ''

                inx = r.getAttribute('index')
                inx = int(re.findall(r'\d+', inx)[0])

                for f in r.getElementsByTagName('F'):
                    if f.getAttribute('L') == u'Unique Identifier':
                        ui = get_tag_text(f, 'D')

                    if f.getAttribute('L') == u'Date Created':
                        test_date = get_tag_text(f, 'D')
                        test_date = datetime.datetime.strptime(test_date.encode('utf-8'), "%Y%m%d")

                # filter based on data
                if test_date < end_date and test_date > start_date:
                    list_ret.append((inx, ui))
                else:
                    print('Document {} in file {} in topic {} does not satisfy year constraint.'.format(inx, mfile, topic_id))

        # sort
        list_ret.sort(key=itemgetter(0))

        # output release data
        with codecs.open(os.path.join(PIDS_DIR, os.path.basename(topic_id)), 'w', 'utf-8') as f:
            for inx, ui in list_ret:
                f.write('{}\n'.format(ui))
    return


def download_title_by_url(review_url):
    """
    Download titles by systematic review url
    :param review_url:
    :return:
    """
    print('processing {}'.format(review_url))

    # start chrome browser
    chromedriver = CHROMEDRIVER_DIR
    os.environ["webdriver.chrome.driver"] = chromedriver
    driver = webdriver.Chrome(executable_path=chromedriver)

    # request and response of website
    driver.get(review_url)

    # get title
    title = driver.find_element_by_xpath('//h1[@class="article-header__title"]').text

    driver.quit()

    return title


def batch_download_title():
    """
    Download title for all the systematic reviews
    :return:
    """
    # read clef reviews
    dict_review = read_ovid_search_file()

    # clear before writing
    with codecs.open(TITLE_DIR, 'w', encoding='utf-8'):
        pass

    # write title
    with codecs.open(TITLE_DIR, 'a', encoding='utf-8') as f:
        for topic_id in dict_review.keys():
            title = download_title_by_url(dict_review[topic_id]['url'])
            f.write('%s ||| %s \n' % (topic_id, title))
    return


def read_title():
    dict_title = {}
    with codecs.open(TITLE_DIR, 'r', 'utf-8') as f:
        for line in f:
            topic_id, title = line.split('|||')
            dict_title[topic_id.strip()] = title.strip()
    return dict_title


def make_release_file(qrel_type):
    """
    Make topic file or qrel file
    :param qrel_type: str. 'topic', 'abs', 'doc'.
    :return:
    """
    # get review relevance
    dict_rel = read_clef_rel(qrel_type=qrel_type)

    # get review dict
    dict_review = read_ovid_search_file()

    # get review titles
    dict_title = read_title()

    for topic_id in get_file_ids(PIDS_DIR):

        assert topic_id in dict_review.keys(), 'topic {} not in medline_ovid_search.xlsx'.format(topic_id)

        # read pids
        with codecs.open(os.path.join(PIDS_DIR, topic_id), 'r', 'utf-8') as fr:
            list_boolean = []
            local = []
            for l in fr:
                if l.strip() not in local:
                    list_boolean.append(l.strip())
                    local.append(l.strip())  # remove duplicate pids and keep the original order

        review_doi = dict_review[topic_id]['review_doi']
        title = dict_title[topic_id]
        query = dict_review[topic_id]['query']

        # write qrel file or topic file
        if 'abs' == qrel_type:
            with codecs.open(os.path.join(ABS_QREL_DIR, topic_id), 'w', 'utf-8') as fw:
                for item in list_boolean:
                    fw.write('%-12s %-2d %-12s %-2s \n' % (review_doi, 0, item.strip(),
                                                           dict_rel[review_doi].get(item.strip(), 0)))

        if 'doc' == qrel_type:
            with codecs.open(os.path.join(DOC_QREL_DIR, topic_id), 'w', 'utf-8') as fw:
                for item in list_boolean:
                    fw.write('%-12s %-2d %-12s %-2s \n' % (review_doi, 0, item.strip(),
                                                           dict_rel[review_doi].get(item.strip(), 0)))

        elif 'topic' == qrel_type:
            with codecs.open(os.path.join(TOPIC_DIR, topic_id), 'w', encoding='utf-8') as fw:
                fw.write('Topic: %s \n\n' % review_doi)
                fw.write('Title: %s \n\n' % title)
                fw.write('Query: \n%s \n\n' % query)
                fw.write('Pids: \n')
                {fw.write('    %s \n' % pid) for pid in list_boolean}
        else:
            pass

    return


def download_abstract():
    """
    Download abstract for all the pids
    :return:
    """
    # read boolean result
    for topic_id in get_file_ids(PIDS_DIR):
        print('processing systematic review {}'.format(topic_id))

        # get pids
        with codecs.open(os.path.join(PIDS_DIR, topic_id), 'r', 'utf-8') as fr:
            list_boolean = []
            local = []
            for l in fr:
                if l.strip() not in local:
                    list_boolean.append(l.strip())
                    local.append(l.strip())  # remove duplicate pids and keep the original order

        # make directory for corpora
        dir_document = os.path.join(CORPORA_DIR, str(topic_id))
        if not os.path.exists(dir_document):
            os.makedirs(dir_document)

        # send request
        for block, pids in enumerate(chunks_by_element(list_boolean, 500)):
            str_pids = ','.join(pids)
            payload = {'db': 'pubmed', 'id': str_pids, 'rettype': 'xml', 'retmode': 'xml'}
            r = requests.get(NCBI_API_URL, params=payload)

            # write
            with codecs.open(os.path.join(dir_document, str(block)), 'w', 'utf-8') as f:
                f.write(r.content.decode('utf-8'))

        sleep(5)

    return


def trec_format_abstract():
    """
    Make the downloaded abstract TRECTEXT format
    :return:
    """
    for topic_id in get_dirs(CORPORA_DIR):

        # make directory for every topic
        dir_document = os.path.join(TRECTEXT_DIR, str(topic_id))
        if not os.path.exists(dir_document):
            os.makedirs(dir_document)

        for mfile in get_file_ids(os.path.join(CORPORA_DIR, topic_id)):

            with codecs.open(os.path.join(TRECTEXT_DIR, topic_id, mfile), 'w', 'utf-8') as f:

                # open xml
                dom = xml.dom.minidom.parse(os.path.join(CORPORA_DIR, topic_id, mfile))

                # get root elements
                root = dom.documentElement

                # read original downloaded data
                for r in root.getElementsByTagName('PubmedArticle'):
                    MedlineCitation = r.getElementsByTagName('MedlineCitation')[0]
                    pid = get_tag_text(MedlineCitation, 'PMID')
                    Article = MedlineCitation.getElementsByTagName('Article')[0]
                    title = get_tag_text(Article, 'ArticleTitle')
                    try:
                        abstract = []
                        abstract_nodes = Article.getElementsByTagName('AbstractText')
                        for abstract_node in abstract_nodes:
                            for node in abstract_node.childNodes:
                                if node.TEXT_NODE == node.nodeType:
                                    abstract.append(node.data)
                        abstract = u'\n'.join(abstract)
                    except:
                        print('AbstractText field does not exist for '.format(pid))

                    # transform to TRECTEXT format
                    f.write(u'<DOC>\n')
                    f.write(u'<DOCNO>{}</DOCNO>\n'.format(pid))
                    f.write(u'<TITLE>{}</TITLE>\n'.format(title))
                    f.write(u'<TEXT>{}</TEXT>\n'.format(abstract))
                    f.write(u'</DOC>\n\n')
    return



def statistics():
    """
    Statistics of the released data
    :return:
    """
    header = ['TOPIC', 'ITERATION', 'DOCUMENT', 'RELEVANCY']
    dir_qrel_abs = ABS_QREL_DIR
    dir_qrel_doc = DOC_QREL_DIR
    stats = []

    print('{:<10} | {:<10} | {:<10} | {:<10} | {:<10} | {:<10} | {:<10}'.format(
        'file name', 'topic', '# total doc', '# abs rel', '# doc rel', '% abs rel', '% doc rel'))

    for topic_id in get_file_ids(dir_qrel_abs):
        # Read qrels
        df_abs = pd.read_csv(os.path.join(dir_qrel_abs, topic_id), sep='\s+', names=header)
        df_content = pd.read_csv(os.path.join(dir_qrel_doc, topic_id), sep='\s+', names=header)

        total_num = df_abs.ix[:, 'TOPIC'].count()
        rel_abs_num = df_abs.loc[df_abs['RELEVANCY'] == 1].ix[:, 'RELEVANCY'].count()
        rel_content_num = df_content.loc[df_content['RELEVANCY'] == 1].ix[:, 'RELEVANCY'].count()

        stats.append((total_num, rel_abs_num, rel_content_num))

        # per topic
        print(r'{:<10} | {:<10} | {:<10} | {:<10} | {:<10} | {:<10.2f} | {:<10.2f}'.format(topic_id,
                                                                    df_abs.iloc[0]['TOPIC'],
                                                                    total_num,
                                                                    rel_abs_num,
                                                                    rel_content_num,
                                                                    float(rel_abs_num) / total_num * 100,
                                                                    float(rel_content_num) / total_num * 100))

    # in total
    sum_total_num = sum(total_num for (total_num, rel_abs_num, rel_content_num) in stats)
    sum_rel_abs_num = sum(rel_abs_num for (total_num, rel_abs_num, rel_content_num) in stats)
    sum_rel_content_num = sum(rel_content_num for (total_num, rel_abs_num, rel_content_num) in stats)

    print('')
    print(r'{:<10} | {:<10} | {:<10} | {:<10} | {:<10} | {:<10.2f} | {:<10.2f}'.format('total', ' ',
                                                                  sum_total_num,
                                                                  sum_rel_abs_num,
                                                                  sum_rel_content_num,
                                                                  float(sum_rel_abs_num) / sum_total_num * 100,
                                                                  float(sum_rel_content_num) / sum_total_num * 100))

    return

if __name__ == '__main__':
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    check_existing()

    batch_download_pid()
    extract_pid()
    batch_download_title()

    make_release_file('topic')
    make_release_file('abs')
    make_release_file('doc')

    download_abstract()

    statistics()