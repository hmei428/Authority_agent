import json
import logging
from typing import Dict, Iterable, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class MetaSearchClient:
    def __init__(
        self,
        api_key: str,
        request_id: str = "search_prime_agent",
        timeout: int = 15,
        pool_connections: int = 50,
        pool_maxsize: int = 50,
    ) -> None:
        self.api_key = api_key
        self.request_id = request_id
        self.url = "https://runway.devops.xiaohongshu.com/openai/zhipu/paas/v4/web_search"
        self.timeout = timeout

        # 创建Session
        self.session = requests.Session()
        self.session.headers.update({"api-key": self.api_key})

        # 配置重试策略
        retry_strategy = Retry(
            total=3,  # 总共重试3次
            backoff_factor=0.5,  # 重试间隔：0.5s, 1s, 2s
            status_forcelist=[429, 500, 502, 503, 504],  # 遇到这些状态码时重试
            allowed_methods=["POST"],  # 允许POST请求重试
        )

        # 配置HTTP适配器，增加连接池大小
        adapter = HTTPAdapter(
            pool_connections=pool_connections,  # 连接池数量
            pool_maxsize=pool_maxsize,  # 每个连接池的最大连接数
            max_retries=retry_strategy,  # 重试策略
        )

        # 挂载适配器到session
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def search(self, query: str) -> List[Dict]:
        search_engine = "search_prime"
        data = {
            "search_engine": search_engine,
            "search_query": query,
            "query_rewrite": "false",
            "request_id": self.request_id,
        }

        # 发送请求（超时时间可配置，默认15秒）
        response = self.session.post(self.url, data=json.dumps(data), timeout=self.timeout)
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
