import json
import logging
from typing import Dict, Iterable, List

import requests

logger = logging.getLogger(__name__)


class MetaSearchClient:
    def __init__(self, api_key: str, request_id: str = "search_prime_agent") -> None:
        self.api_key = api_key
        self.request_id = request_id
        self.url = "https://runway.devops.xiaohongshu.com/openai/zhipu/paas/v4/web_search"
        # 优化1+3：复用session，减少连接开销
        self.session = requests.Session()
        self.session.headers.update({"api-key": self.api_key})

    def search(self, query: str) -> List[Dict]:
        search_engine = "search_pro_ms"
        data = {
            "search_engine": search_engine,
            "search_query": query,
            "query_rewrite": "false",
            "request_id": self.request_id,
        }
        # 优化3：timeout 15s -> 8s
        response = self.session.post(self.url, data=json.dumps(data), timeout=8)
        response.raise_for_status()
        payload = response.json()
        items: Iterable[Dict] = payload.get("search_result", []) or []

        # 在每个结果中添加search_engine字段（如果API返回中没有的话）
        results = []
        for item in items:
            if "search_engine" not in item:
                item["search_engine"] = search_engine
            results.append(item)
        return results
