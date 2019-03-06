import logging
import random
import sys
import time
import errno
import os
import requests
import requests.exceptions
from bs4 import BeautifulSoup
from etl_register import ETLCrawler

logger = logging.getLogger(__name__)

NEW_YORK_TIMES_ECONOMY_RAW_PATH =  os.path.join('data','nyt_economy')
PARSER_CSV_PATH = os.path.join('data','parser_csv')
URL_NEWS_NEW_YORK_TIMES_ECONOMY = 'https://www.nytimes.com/section/business/economy'

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def random_time_sleep(min_sec=1, max_sec=5):
    delay = random.randint(min_sec, max_sec)
    time.sleep(delay)

def getSoupElementText(element):
    if(element):
        text = element.text
        return removeTextNewline(text)
    else:
        return None

def removeTextNewline(text):
    return text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')

def get_files (trgt_path, sort_by_mtime='desc'):
    '''return list of file names of input path
    Args: trgt_path (string): target path
    '''
    #return [join(trgt_path, f) for f in listdir(trgt_path) if isfile(join(trgt_path, f))]
    name_set = []
    for dir_path, dir_names, file_names in os.walk(trgt_path):
        for file_name in file_names:
            a_file = os.path.join(dir_path, file_name)
            if not os.path.isdir(a_file) and os.path.isfile(a_file):
                name_set.append(os.path.join(dir_path, file_name))
    if sort_by_mtime == 'desc':
        name_set.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    elif sort_by_mtime == 'asc':
        name_set.sort(key=lambda x: os.path.getmtime(x), reverse=False)
    return name_set

class new_york_times_economy(ETLCrawler):

    execute_cron_time = "00 20 * * *"

    @ETLCrawler.register_extract(1)
    def extract(self, ds, **task_kwargs):
        mkdir_p(NEW_YORK_TIMES_ECONOMY_RAW_PATH)
        self._crawl(ds,NEW_YORK_TIMES_ECONOMY_RAW_PATH)

    @ETLCrawler.register_transform(1)
    def transform(self, *args, **kwargs):
        mkdir_p(PARSER_CSV_PATH)
        csv_data = self._parse(NEW_YORK_TIMES_ECONOMY_RAW_PATH)
        self.write_csv(csv_data,PARSER_CSV_PATH)

    @ETLCrawler.register_load(1)
    def load(self, *args, **kwargs):
        for row in self.pre_load(PARSER_CSV_PATH):
            print(row)

    def _crawl(self, tx_dt, raw_dir):
        links = self._crawl_article_url_list(URL_NEWS_NEW_YORK_TIMES_ECONOMY)        
        for link in links:
            file_name = link.split('/')[-1]            
            self._crawl_article(tx_dt, link, file_name,
                                raw_dir)
            """ IT:break """

    def _crawl_article_url_list(self, url):
        try:
            result = requests.get(url)
            c = result.content
            soup = BeautifulSoup(c, 'lxml')
            section = soup.select(
                'section#collection-business-economy section li a')
            article_links = [a['href'] if 'www.nytimes.com' in a['href']
                             else 'https://www.nytimes.com' + a['href']
                             for a in section]
            return article_links
        except requests.exceptions.Timeout:
            random_time_sleep()
            self._crawl_article_url_list(url)
        except requests.exceptions.RequestException as e:
            sys.exit(1)

    def _crawl_article(self, tx_dt, article_url, file_name,
                       saving_path):
        """
        Args:
            tx_dt (str): YYYY-MM-DD
            article_url (str): article_url
        """

        result = requests.get(article_url)
        with open(os.path.join(saving_path,file_name), 'wb') as f:
            f.write(result.content)

    def _parse(self, saving_path):
        files = get_files(saving_path)
        row_list = []
        for _file in files:
            with open(_file, 'r') as file:
                try:
                    row_list.append(self._parse_article(file))
                except Exception as e:
                    logger.error(file.name)
                    logger.error(e)
                    continue
        return row_list

    def _parse_article(self, file):
        soup = BeautifulSoup(file, 'lxml')
        sub_section = title = author = publish_date = tx_dt = outline = context = ""
        publish_date = soup.select_one('meta[itemprop=datePublished]')[
            'content'].split('T')[0]
        tx_dt = soup.select_one('meta[itemprop=dateModified]')[
            'content'].split('T')[0]
        if soup.select_one('meta[itemprop=description]'):
            outline = removeTextNewline(soup.select_one(
                'meta[itemprop=description]')['content'])
        if soup.select_one('div#app'):  # mobile webpage
            sub_section = getSoupElementText(soup.select_one(
                'div[class^="SectionBar-sectionBarHeading"]'))
            title = getSoupElementText(soup.select_one(
                'h1 span'))
            author = getSoupElementText(soup.select_one(
                'a[class^="Byline-bylineAuthor"]'))
            if not author:
                author = getSoupElementText(soup.select_one('p[itemprop="author creator"] span[itemprop="name"]'))
            context = '\t'.join(
                [p.text for p in soup.select('article#story header ~ p')])
            if not context:
                context = '\t'.join(
                    [p.text for p in soup.select('article#story div.StoryBodyCompanionColumn p')])
        # interactive page (still got h2)
        elif soup.select_one('html.page-interactive'):
            sub_section = getSoupElementText(
                soup.select_one('span.kicker-label a'))
            title = getSoupElementText(
                soup.select_one('.interactive-headline'))
            author = getSoupElementText(
                soup.select_one('span.byline-author'))
            if soup.select_one('p.g-body'):
                context = '\t'.join(
                    [p.text for p in soup.select('p.g-body')])
            elif soup.select_one('.listy_body'):
                context = '\t'.join(
                    [p.text for p in soup.select('.intro-content-wrap > .listy_body p')])
                if soup.select_one('li.row.list_item'):
                    list_item = soup.select('li.row.list_item')
                    for li in list_item:
                        sub_headline = ""
                        sub_context = ""
                        if li.select_one('.listy_headline'):
                            sub_headline = "\t SUB_HEADLINE:" + \
                                getSoupElementText(
                                    li.select_one('.listy_headline')) + "\t"
                        if li.select_one('.listy_body p'):
                            sub_context = "\t".join(
                                [p.text for p in li.select('.listy_body p')])
                        context = context + sub_headline + sub_context
            elif soup.select_one("div.rad-story-body"):
                context = '\t'.join(
                    [p.text for p in soup.select("div.rad-story-body p")])                    
        else:
            sub_section = getSoupElementText(
                soup.select_one('span.kicker-label a'))
            title = getSoupElementText(soup.select_one('#headline'))
            author = getSoupElementText(
                soup.select_one('span.byline-author'))
            context = '\t'.join(
                [p.text for p in soup.select('p.story-body-text.story-content')])
        context = removeTextNewline(context)
        url = soup.select_one('html')['itemid']
        row = (sub_section, title, author, tx_dt,
               publish_date, context, url, outline)        
        return row


if __name__ == '__main__':
    myETL = new_york_times_economy()
    myETL.extract(sys.argv[1])
    myETL.transform()
    myETL.load()