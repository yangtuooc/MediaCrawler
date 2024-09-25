from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

@dataclass
class CrawlerContext:
    platform: str
    crawler_type: str
    keywords: str
    login_type: str
    headless: bool
    save_login_state: bool
    save_data_option: str
    crawler_max_notes_count: int
    enable_get_images: bool
    enable_get_comments: bool
    enable_get_sub_comments: bool
    enable_ip_proxy: bool
    ip_proxy_pool_count: int
    ip_proxy_provider_name: str
    user_data_dir: str
    start_page: int
    max_concurrency_num: int
    enable_get_wordcloud: bool
    stop_words_file: str
    font_path: str
    cookies: str
    sort_type: str = ""
    xhs_creator_id_list: List[str] = field(default_factory=list)
    xhs_specified_id_list: List[str] = field(default_factory=list)
    dy_creator_id_list: List[str] = field(default_factory=list)
    dy_specified_id_list: List[str] = field(default_factory=list)
    ks_creator_id_list: List[str] = field(default_factory=list)
    ks_specified_id_list: List[str] = field(default_factory=list)
    wb_creator_id_list: List[str] = field(default_factory=list)
    wb_specified_id_list: List[str] = field(default_factory=list)
    zhihu_creator_id_list: List[str] = field(default_factory=list)
    zhihu_specified_id_list: List[str] = field(default_factory=list)
    tieba_creator_id_list: List[str] = field(default_factory=list)
    tieba_specified_id_list: List[str] = field(default_factory=list)
    bili_creator_id_list: List[str] = field(default_factory=list)
    bili_specified_id_list: List[str] = field(default_factory=list)
    # 添加一个字段来存储额外的配置
    extra_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, config: dict):
        # 创建一个新的字典,只包含CrawlerContext中定义的字段
        context_fields = {f.name for f in cls.__dataclass_fields__.values()}
        context_config = {k: v for k, v in config.items() if k in context_fields}
        
        # 将其他未定义的字段放入extra_config
        extra_config = {k: v for k, v in config.items() if k not in context_fields}
        context_config['extra_config'] = extra_config

        return cls(**context_config)

    def __getattr__(self, name):
        # 如果属性不存在,尝试从extra_config中获取
        if name in self.extra_config:
            return self.extra_config[name]
        raise AttributeError(f"'CrawlerContext' object has no attribute '{name}'")