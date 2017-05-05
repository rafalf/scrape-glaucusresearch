#!/usr/bin/env python


# win: pip install lxml==3.6.0  (other pip install lxml)
# pip install requests
# pip install beautifulsoup4

import requests
from requests import ConnectionError
from bs4 import BeautifulSoup
import os
import sys
import getopt
import logging
import time
import csv

scrape_urls = ['https://glaucusresearch.com/corporate-corruption/',
               'https://glaucusresearch.com/consumer-watchdog-reports/']

logger = logging.getLogger(os.path.basename(__file__))


def scrape(fld, from_date, to_date):

    total_counter = 0

    for scrape_url in scrape_urls:

        logger.info('Scraping: {} for all articles'.format(scrape_url))
        r = requests.get(scrape_url)

        # create download folder
        if not fld:
            downloads_folder = os.path.join(os.path.dirname(__file__), 'download')
        else:
            downloads_folder = os.path.join(os.path.dirname(__file__), fld)
        if not os.path.isdir(downloads_folder):
            os.mkdir(downloads_folder)

        soup = BeautifulSoup(r.text, 'lxml')
        main_ = soup.find(id="main")

        articles = []
        for section_h1 in main_.select('h1'):

            metadata = []
            logger.info('Heading (company): {}'.format(section_h1.text.encode('utf-8')))
            next_node = section_h1

            while True:
                next_node = next_node.next_sibling
                try:
                    tag_name = next_node.name
                except AttributeError:
                    tag_name = ""
                if tag_name == "p":
                    if next_node.find('em'):

                        find_em = next_node.find('em')
                        if find_em and len(find_em.text) > 2:
                            date_ = find_em.text
                            logger.info('Date found: {}'.format(date_))
                            metadata.append(time.strftime('%B %d, %Y', time.localtime()))
                            metadata.append(section_h1.text.encode('ascii', errors='ignore'))
                            metadata.append(date_)

                            post_date = time.strptime(date_, '%B %d, %Y')
                            post_date_secs = time.mktime(post_date)
                            logger.info('Date in secs: %s' % post_date_secs)
                            set_date = True

                            if from_date < post_date_secs < to_date:
                                logger.info('Between start and end date -> Process')

                                # a link inside <p> tag
                                # p>em
                                # p>a
                                if next_node.find('a') and set_date:
                                    find_a = next_node.find('a')
                                    if find_a and len(find_a.text) > 2:
                                        title_ = find_a.text.encode('utf-8')
                                        logger.info('Link found: {}'.format(title_))
                                        metadata.append(title_)

                                        href_ = find_a.get('href')
                                        logger.info('Href found: {}'.format(href_))
                                        metadata.append(href_)

                                        set_date = False
                                        articles.append(metadata)
                                        metadata = []
                                        continue

                            else:
                                logger.info('Not between start and end date -> Skip')
                                set_date = False
                                metadata = []
                                continue

                    if next_node.find('a') and set_date:
                        find_a = next_node.find('a')
                        if find_a and len(find_a.text) > 2:
                            title_ = find_a.text.encode('utf-8')
                            logger.info('Link found: {}'.format(title_))
                            metadata.append(title_)

                            href_ = find_a.get('href')
                            logger.info('Href found: {}'.format(href_))
                            metadata.append(href_)

                            set_date = False
                            articles.append(metadata)
                            metadata = []
                            continue
                else:
                    logger.info('*****')
                    break

        for metadata_ in articles:

            for _ in range(3):

                try:
                    # get requests
                    s = requests.Session()

                    href_ = metadata_[4]
                    request = s.get(href_, timeout=30, stream=True, cookies={'toscheck': 'yes'})
                    file_name = href_[href_.rfind('/') + 1:]

                    if file_name.count('.pdf'):

                        # create folders
                        split = metadata_[2].split(' ')
                        year_numeric = split[2]
                        month_alphabetic = time.strptime(split[0], "%B")
                        month_numeric = time.strftime("%m", month_alphabetic)
                        day_without_leading = time.strptime(split[1].strip(','), "%d")
                        day_numeric = time.strftime("%d", day_without_leading)
                        folder_struc = os.path.join(downloads_folder, metadata_[1][:metadata_[1].find('(')].strip(), year_numeric,
                                                    month_numeric, day_numeric)

                        if not os.path.isdir(folder_struc):
                            os.makedirs(folder_struc)
                            logger.info('Folders created: %s' % folder_struc)

                        file_ = os.path.join(folder_struc, file_name)
                        with open(file_, 'wb') as fh:
                            for chunk in request.iter_content(chunk_size=1024):
                                fh.write(chunk)
                        logger.info('Downloaded as: {}'.format(file_))

                        row = ['Process Date', 'Company Name', 'Publish Date', 'Article Title', 'Article URL']
                        metadata_file = file_name[:-4] + '.csv'
                        _write_row(row, os.path.join(folder_struc, metadata_file))
                        _write_row(metadata_, os.path.join(folder_struc, metadata_file))
                        logger.info('Metadata saved as: {}'.format(os.path.join(folder_struc, metadata_file)))

                        total_counter += 1
                    else:
                        logger.info('Not a pdf'.format(file_))
                    break
                except ConnectionError:
                    logger.info('ConnectionError --> retry up to 3 times')
                except Exception:
                    logger.info('UndefinedException --> retry up to 3 times')
                    logger.debug('Stack trace: -->', exc_info=True)
            else:
                logger.error('ERROR: Failed to download')
                logger.error(metadata_)

    logger.info('Total articles saved: {}'.format(total_counter))


def _write_row(row, full_path):
    with open(full_path, 'ab') as hlr:
        wrt = csv.writer(hlr, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        wrt.writerow(row)
        logger.debug('Added to %s file: %s' % (full_path, row))


if __name__ == '__main__':
    download_folder = None
    verbose = None
    from_date = '01/01/1970'
    to_date = '01/01/2100'

    log_file = os.path.join(os.path.dirname(__file__), 'logs',
                                time.strftime('%d%m%y', time.localtime()) + "_scraper.log")
    file_hndlr = logging.FileHandler(log_file)
    logger.addHandler(file_hndlr)
    console = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(console)
    ch = logging.Formatter('[%(levelname)s] %(message)s')
    console.setFormatter(ch)
    file_hndlr.setFormatter(ch)

    argv = sys.argv[1:]
    opts, args = getopt.getopt(argv, "o:vf:t", ["output=", "verbose", "from=", "to="])
    for opt, arg in opts:
        if opt in ("-o", "--output"):
            download_folder = arg
        elif opt in ("-f", "--from"):
            from_date = arg
        elif opt in ("-t", "--to"):
            to_date = arg
        elif opt in ("-v", "--verbose"):
            verbose = True

    str_time = time.strptime(from_date, '%m/%d/%Y')
    from_secs = time.mktime(str_time)

    str_time = time.strptime(to_date, '%m/%d/%Y')
    to_secs = time.mktime(str_time)

    if verbose:
        logger.setLevel(logging.getLevelName('DEBUG'))
    else:
        logger.setLevel(logging.getLevelName('INFO'))

    logger.info('CLI args: {}'.format(opts))
    logger.info('from: {}'.format(from_date))
    logger.info('to: {}'.format(to_date))
    logger.debug('from_in_secs: {}'.format(from_secs))
    logger.debug('to_in_secs: {}'.format(to_secs))

    scrape(download_folder, from_secs, to_secs)