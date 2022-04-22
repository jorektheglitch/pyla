import asyncio
from concurrent.futures import ProcessPoolExecutor, Executor

import aiohttp
from bs4 import BeautifulSoup, element


PARSING_EXECUTOR = ProcessPoolExecutor()

TOPICS_SELECTOR = "#page-body > div.forumbg > div > ul.topiclist.topics > li.row"
TOPIC_TITLE = "li > dl > dt > div > a.topictitle"
TOPIC_LAST_TIME = "li > dl > dd.lastpost > span > time"
TOPIC_LAST_UNAME1 = "li > dl > dd.lastpost > span > a.username"
TOPIC_LAST_UNAME2 = "li > dl > dd.lastpost > span > a.username-coloured"


class Pages:
    MAIN = "https://lizaalert.org/forum/"
    ACTIVE_SEARCHES = "https://lizaalert.org/forum/viewforum.php?f=276"


def get_topics_info(page: BeautifulSoup):
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


if __name__ == "__main__":
    async def main():
        async with aiohttp.ClientSession() as session:
            async with session.get(Pages.ACTIVE_SEARCHES) as resp:
                content = await resp.read()
            page = BeautifulSoup(content, "lxml")
            topics = get_topics_info(page)
            for topic in topics:
                print(topic["title"])
                print(topic["link"])
                msg = topic["last_message"]
                print(msg["time"], msg["from"])
                print()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
