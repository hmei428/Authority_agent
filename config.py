"""
配置管理模块
集中管理OSS配置、API密钥、路径配置等
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class OssConfig:
    """OSS配置"""
    endpoint: str
    access_key_id: str
    access_key_secret: str
    bucket_name: str

    @classmethod
    def from_env(cls) -> "OssConfig":
        """从环境变量读取OSS配置"""
        return cls(
            endpoint=os.getenv("OSS_ENDPOINT", "oss-cn-shanghai.aliyuncs.com"),
            access_key_id=os.getenv("OSS_ACCESS_KEY_ID", ""),
            access_key_secret=os.getenv("OSS_ACCESS_KEY_SECRET", ""),
            bucket_name=os.getenv("OSS_BUCKET", ""),
        )

    def validate(self) -> None:
        """验证配置是否完整"""
        if not all([self.endpoint, self.access_key_id, self.access_key_secret, self.bucket_name]):
            raise ValueError("OSS配置不完整，请检查环境变量：OSS_ENDPOINT, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_BUCKET")


@dataclass
class ApiConfig:
    """API配置"""
    zhipu_api_key: str  # 元搜索API Key
    llm_api_key: str    # 打分LLM API Key
    llm_base_url: str   # 打分LLM Base URL
    llm_model: str      # 打分LLM 模型

    @classmethod
    def from_env(cls) -> "ApiConfig":
        """从环境变量读取API配置"""
        return cls(
            zhipu_api_key=os.getenv("ZHIPU_API_KEY", ""),
            llm_api_key=os.getenv("DIRECT_LLM_API_KEY", ""),
            llm_base_url=os.getenv("DIRECT_LLM_BASE_URL", "http://redservingapi.devops.xiaohongshu.com/v1"),
            llm_model=os.getenv("AUTHORITY_MODEL", "qwen3-30b-a3b"),
        )

    def validate(self) -> None:
        """验证配置是否完整"""
        if not self.zhipu_api_key:
            raise ValueError("ZHIPU_API_KEY 未设置")
        if not self.llm_api_key:
            raise ValueError("DIRECT_LLM_API_KEY 未设置")


@dataclass
class PipelineConfig:
    """流程配置"""
    topk: int                      # 每个query返回的结果数
    authority_threshold: int       # 权威性阈值（1-4）
    relevance_threshold: int       # 相关性阈值（0-2）
    max_workers: int              # 并发数
    filter_authority_score: int   # 第三个CSV筛选条件：权威性评分
    filter_relevance_score: int   # 第三个CSV筛选条件：相关性评分

    @classmethod
    def from_args(
        cls,
        topk: int = 10,
        authority_threshold: int = 2,
        relevance_threshold: int = 1,
        max_workers: int = 8,
        filter_authority_score: int = 4,
        filter_relevance_score: int = 2,
    ) -> "PipelineConfig":
        """从参数创建配置"""
        return cls(
            topk=topk,
            authority_threshold=authority_threshold,
            relevance_threshold=relevance_threshold,
            max_workers=max_workers,
            filter_authority_score=filter_authority_score,
            filter_relevance_score=filter_relevance_score,
        )
