import json
import os
from queue import Queue
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from bs4.element import Comment, NavigableString, Script, Tag
from utils import ds, dt, filex, hashx, jsonx, www

from web_mirror._utils import log

WEB_MIRROR_LOCAL_DIR = '/tmp/web_mirror'
BYTES_IN_KILO_BYTE = 1000
ID_KEBAB, ID_HASH = 16, 8
MAX_URLS_TO_PARSE = 5


def get_id(url):
    url_parts = urlparse(url)
    netloc = url_parts.netloc
    path = url_parts.path

    dir = dt.to_kebab(netloc)[:ID_KEBAB] + '-' + hashx.md5(netloc)[:ID_HASH]
    sub_dir = 'page-' + dt.to_kebab(path)[:ID_KEBAB] + '-' + hashx.md5(url)[:ID_HASH]

    dir = dir.replace('--', '-')
    sub_dir = sub_dir.replace('--', '-')

    return dir, sub_dir


def get_file(url, ext):
    dir, sub_dir = get_id(url)
    dir_part = os.path.join(
        WEB_MIRROR_LOCAL_DIR,
        dir,
    )
    sub_dir_part = os.path.join(
        WEB_MIRROR_LOCAL_DIR,
        dir,
        sub_dir,
    )

    if not os.path.exists(WEB_MIRROR_LOCAL_DIR):
        os.mkdir(WEB_MIRROR_LOCAL_DIR)
        log.info(f'Created dir {WEB_MIRROR_LOCAL_DIR}')
    if not os.path.exists(dir_part):
        os.mkdir(dir_part)
        log.info(f'Created dir {dir_part}')
    if not os.path.exists(sub_dir_part):
        os.mkdir(sub_dir_part)
        log.info(f'Created dir {sub_dir_part}')

    return os.path.join(
        sub_dir_part,
        f'{sub_dir}.{ext}',
    )


def download(url):
    log.info(f'Downloading {url}...')
    html = www.read(url, use_selenium=True)
    html_file = get_file(url, 'html')
    filex.write(html_file, html)
    file_size = len(html) / BYTES_IN_KILO_BYTE
    log.info(f'Downloaded {file_size:.1f}KB from {url} to {html_file}')


def extract_docjson(url):
    html_file = get_file(url, 'html')
    html = filex.read(html_file)

    soup = BeautifulSoup(html, 'html.parser')

    def extract_element_docjson(elem):
        if isinstance(elem, Comment):
            return None
        if isinstance(elem, Script):
            return None

        if isinstance(elem, NavigableString):
            text = str(elem).strip()
            if len(text) > 0:
                return {
                    'text': str(elem),
                }

        elif isinstance(elem, Tag):
            if elem.name in ['script']:
                return None

            children = []
            for child in elem:
                child_docjson = extract_element_docjson(child)
                if child_docjson:
                    children.append(child_docjson)
            attrs = elem.attrs
            if children or attrs:
                return {
                    'tag': elem.name,
                    'children': children,
                    'attrs': attrs,
                }

        return None

    docjson = extract_element_docjson(soup.find('body'))
    docjson_file = get_file(url, 'doc.json')
    jsonx.write(docjson_file, docjson)

    file_size = len(json.dumps(docjson)) / BYTES_IN_KILO_BYTE
    log.info(f'Downloaded {file_size:.1f}KB from {url} to {docjson_file}')

    a_list = soup.find_all('a')
    link_urls_raw = list(
        map(
            lambda a: a.attrs.get('href', ''),
            a_list,
        )
    )
    link_urls = []
    for link_url in link_urls_raw:
        if link_url == '#':
            continue
        if link_url[-1] == '/':
            link_url = link_url[:-1]
        if url not in link_url:
            link_url = f'{url}/{link_url}'
        link_urls.append(link_url)

    link_urls = ds.unique(link_urls)
    link_urls = sorted(link_urls)
    link_urls_file = get_file(url, 'links.json')
    jsonx.write(link_urls_file, link_urls)
    n_links = len(link_urls)
    log.info(f'Wrote {n_links} links from {url} to {link_urls_file}')
    return link_urls


def docjson_to_md(docjson):
    text = docjson.get('text', '')
    tag = docjson.get('tag')

    child_md = ''
    for child in docjson.get('children', []):
        child_md += docjson_to_md(child)
    all_text = f'{text} {child_md}'

    if tag == 'li':
        return f'\n* {all_text}'
    if tag == 'div':
        return f'\n{all_text}\n'
    if tag == 'p':
        return f'\n{all_text}\n'

    if tag == 'span':
        return f' {all_text} '

    if tag == 'i':
        return f'*{all_text}*'
    if tag == 'strong':
        return f'**{all_text}**'

    if tag == 'a':
        href = docjson.get('attrs', {}).get('href', '')
        if all_text:
            label = all_text.replace('\n', ' ').strip()
        else:
            label = href
        return f'[{label}]({href})'

    if tag == 'img':
        src = docjson.get('attrs', {}).get('src', '')
        alt = docjson.get('attrs', {}).get('alt', '')
        return f'![{alt}]({src})'

    for i in range(0, 6):
        h_tag = 'h%d' % (i + 1)
        if h_tag == tag:
            result = '\n%s %s\n' % (
                '#' * (i + 1),
                all_text,
            )
            return result

    return f'{all_text}'


def extract_md(url):
    docjson_file = get_file(url, 'doc.json')
    docjson = jsonx.read(docjson_file)

    md = docjson_to_md(docjson)

    md = '\n'.join(
        list(
            filter(
                lambda line: len(line) > 0,
                list(
                    map(
                        lambda line: line.strip(),
                        md.split('\n'),
                    )
                ),
            )
        )
    )

    md_file = get_file(url, 'md')
    filex.write(md_file, md)
    file_size = len(md) / BYTES_IN_KILO_BYTE
    log.info(f'Downloaded {file_size:.1f}KB from {url} to {md_file}')


def scrape(root_url):
    visited_urls_set = set()
    queued_urls_set = set()
    url_queue = Queue()
    url_queue.put(root_url)

    while url_queue.qsize() > 0:
        current_url = url_queue.get()
        download(current_url)
        link_urls = extract_docjson(current_url)
        extract_md(current_url)

        visited_urls_set.add(current_url)
        if len(visited_urls_set) > MAX_URLS_TO_PARSE:
            break

        for link_url in link_urls:
            if (
                root_url in link_url
                and link_url not in visited_urls_set
                and link_url not in queued_urls_set
            ):
                url_queue.put(link_url)
                queued_urls_set.add(link_url)


if __name__ == '__main__':
    # scrape('https://www.peps.lk')
    scrape('https://www.colombo.mc.gov.lk')
