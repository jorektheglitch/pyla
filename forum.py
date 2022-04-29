import asyncio
from concurrent.futures import Executor, ProcessPoolExecutor
from functools import wraps, partial

from typing import Any, Optional, TypeVar, Union
from typing import Callable, Coroutine, List

import aiohttp
from bs4 import BeautifulSoup, element


PARSING_EXECUTOR = ProcessPoolExecutor()

TOPICS_SELECTOR = "#page-body > div.forumbg > div > ul.topiclist.topics > li.row"
TOPIC_TITLE = "li > dl > dt > div > a.topictitle"
TOPIC_LAST_TIME = "li > dl > dd.lastpost > span > time"
TOPIC_LAST_UNAME1 = "li > dl > dd.lastpost > span > a.username"
TOPIC_LAST_UNAME2 = "li > dl > dd.lastpost > span > a.username-coloured"

POSTS_SELECTOR = "#page-body > div.post"
# POST_UNAME = "div.post > div > dl > dt > a"
POST_AUTHOR = "div.post > div.inner > dl > dt > a"
POST_TIME = "div.post > div.inner > div.postbody > div > p.author > time"
POST_CONTENT = "div.post > div.inner > div.postbody > div > div.content"

T = TypeVar("T")


class Pages:
    MAIN = "https://lizaalert.org/forum/"
    ACTIVE_SEARCHES = "https://lizaalert.org/forum/viewforum.php?f=276"


def as_async(fn: Callable[..., T], executor: Optional[Executor] = None) -> Callable[..., Coroutine[Any, Any, T]]:
    @wraps(fn)
    async def wrapped(*args, **kwargs):
        loop = asyncio.get_running_loop()
        f = partial(fn, *args, **kwargs)
        fut = loop.run_in_executor(executor, f)
        return await fut
    return wrapped


def parse_topics_list(content: Union[str, bytes], features="lxml"):
    page = BeautifulSoup(content, features)
    topics_elements = page.select(TOPICS_SELECTOR)
    topics = []
    for topic_el in topics_elements:
        if isinstance(topic_el, element.Tag):
            info = {}
            topic_title = topic_el.select_one(TOPIC_TITLE)
            info["title"] = topic_title.text
            info["link"] = topic_title.attrs.get("href")
            info["last_message"] = last_message = {}
            message_from_usual = topic_el.select_one(TOPIC_LAST_UNAME1)
            message_from_coloured = topic_el.select_one(TOPIC_LAST_UNAME2)
            last_message_from = message_from_usual or message_from_coloured
            last_message["from"] = last_message_from.text
            last_message_time = topic_el.select_one(TOPIC_LAST_TIME)
            last_message["time"] = last_message_time.attrs.get("datetime")
            topics.append(info)
    return topics


def parse_topic_posts(content: Union[str, bytes], features="lxml"):
    page = BeautifulSoup(content, features)
    posts_elements = page.select(POSTS_SELECTOR)
    posts = []
    for post_el in posts_elements:
        post = {}
        post["author"] = author = {}
        post_author = post_el.select_one(POST_AUTHOR)
        author["link"] = post_author.attrs.get("href")
        author["name"] = post_author.text
        post_time = post_el.select_one(POST_TIME)
        post["time"] = post_time.attrs.get("datetime")
        content = post_el.select_one(POST_CONTENT)
        post["text_rows"] = get_rows(content)
        posts.append(post)
    return posts


def get_rows(el: element.Tag) -> List[str]:
    rows = []
    for child in el.children:
        if isinstance(child, element.Tag):
            text = child.text
            if text:
                rows.append(text)
        elif isinstance(child, str):
            if child:
                rows.append(child)
    return rows


if __name__ == "__main__":
    async def main():
        async with aiohttp.ClientSession() as session:
            async with session.get(Pages.ACTIVE_SEARCHES) as resp:
                content = await resp.read()
            topics = parse_topics_list(content)
            for topic in topics:
                print(topic["title"])
                print(topic["link"])
                msg = topic["last_message"]
                print(msg["time"], msg["from"])
                print()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
