import re
from time import sleep
from typing import TYPE_CHECKING

import aswan
from aswan import get_soup
from aswan.utils import add_url_params

if TYPE_CHECKING:
    from bs4 import BeautifulSoup

SLEEP_TIME = 3

project = aswan.Project(
    name="ingatlan",
    distributed_api="sync",
    max_cpu_use=1,
)


@project.register_handler
class AdHandler(aswan.RequestHandler):
    url_root = "https://ingatlan.com"

    def parse(self, blob):
        sleep(SLEEP_TIME)
        return blob


@project.register_handler
class ListingHandler(aswan.RequestSoupHandler):
    url_root = "https://ingatlan.com/lista/kiado+lakas+budapest"

    def parse(self, soup: "BeautifulSoup"):
        sleep(SLEEP_TIME)
        ad_ids = [int(e.get("data-id")) for e in soup.select(".listing")]
        self.register_links_to_handler(
            links=[f"{AdHandler.url_root}/{ad_id}" for ad_id in ad_ids],
            handler_cls=AdHandler,
        )
        return ad_ids


def parse_page_count(soup: "BeautifulSoup") -> int:
    pagination_text = soup.select_one(".pagination__page-number").get_text(strip=True)
    return int(re.search(r"(\d+) / (\d+) oldal", pagination_text).group(2))


if __name__ == "__main__":
    soup = get_soup(url=ListingHandler.url_root)
    all_page = range(1, parse_page_count(soup=soup) + 1)

    project.start_monitor_process()
    project.run(
        urls_to_register={
            ListingHandler: [
                add_url_params(ListingHandler.url_root, {"page": p}) for p in all_page
            ][:10]
        }
    )
    project.commit_current_run()
    project.stop_monitor_process()
