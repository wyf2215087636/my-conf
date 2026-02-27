#!/usr/bin/env python3
import json
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ES_URL = "http://elasticsearch:9200"
INDEX_NAME = "address_places_v2"


def wait_for_elasticsearch(url=ES_URL, timeout=300):
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = session.get(
                f"{url}/_cluster/health",
                params={"wait_for_status": "yellow", "timeout": "10s"},
                timeout=15,
            )
            if resp.status_code == 200:
                print("Elasticsearch is ready.")
                return True
        except requests.exceptions.RequestException:
            pass
        print("Waiting for Elasticsearch...")
        time.sleep(5)
    raise RuntimeError(f"Elasticsearch not ready after {timeout}s")


def create_index(es_url=ES_URL, index_name=INDEX_NAME):
    try:
        resp = requests.head(f"{es_url}/{index_name}", timeout=10)
        if resp.status_code == 200:
            print(f"Index '{index_name}' already exists, skip create.")
            return True
    except requests.exceptions.RequestException:
        pass

    index_config = {
        "settings": {
            "index.max_ngram_diff": 10,
            "index.mapping.total_fields.limit": 2000,
            "analysis": {
                "analyzer": {
                    "myanmar_ngram": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "my_ngram"],
                    },
                    "generic_edge": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "edge_2_15"],
                    },
                },
                "filter": {
                    "my_ngram": {"type": "ngram", "min_gram": 2, "max_gram": 8},
                    "edge_2_15": {"type": "edge_ngram", "min_gram": 2, "max_gram": 15},
                },
            },
        },
        "mappings": {
            "properties": {
                "osm_type": {"type": "keyword"},
                "osm_id": {"type": "long"},
                "place_id": {"type": "long"},
                "class": {"type": "keyword"},
                "type": {"type": "keyword"},
                "admin_level": {"type": "integer"},
                "rank_address": {"type": "integer"},
                "rank_search": {"type": "integer"},
                "importance": {"type": "double"},
                "country_code": {"type": "keyword"},
                "postcode": {"type": "keyword"},
                "indexed_date": {"type": "date"},
                "centroid": {"type": "geo_point"},
                "names": {
                    "properties": {
                        "name_default": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 512},
                                "ngram": {
                                    "type": "text",
                                    "analyzer": "generic_edge",
                                    "search_analyzer": "standard",
                                },
                            },
                        },
                        "name_my": {
                            "type": "text",
                            "analyzer": "myanmar_kytea_analyzer",
                            "search_analyzer": "myanmar_kytea_analyzer",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 512},
                                "ngram": {
                                    "type": "text",
                                    "analyzer": "myanmar_ngram",
                                    "search_analyzer": "myanmar_ngram",
                                },
                            },
                        },
                        "name_en": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 512},
                                "ngram": {
                                    "type": "text",
                                    "analyzer": "generic_edge",
                                    "search_analyzer": "standard",
                                },
                            },
                        },
                        "name_zh": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 512},
                                "ngram": {
                                    "type": "text",
                                    "analyzer": "generic_edge",
                                    "search_analyzer": "standard",
                                },
                            },
                        },
                    }
                },
                "address": {
                    "properties": {
                        "country_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "country_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "country_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "city_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "city_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "city_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "region_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "region_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "region_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "road_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "road_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "road_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "building_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "building_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "building_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "house_number": {"type": "keyword"},
                        "postcode": {"type": "keyword"},
                    }
                },
                "search": {
                    "properties": {
                        "full_my": {"type": "text", "analyzer": "myanmar_kytea_analyzer"},
                        "full_en": {"type": "text"},
                        "full_zh": {"type": "text"},
                        "tokens": {"type": "keyword"},
                    }
                },
                "address_parts": {
                    "type": "nested",
                    "properties": {
                        "address_place_id": {"type": "long"},
                        "osm_type": {"type": "keyword"},
                        "osm_id": {"type": "long"},
                        "rank": {"type": "integer"},
                        "part_class": {"type": "keyword"},
                        "part_type": {"type": "keyword"},
                        "name": {
                            "properties": {
                                "name_default": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                                "name_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                                "name_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                                "name_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            }
                        },
                    },
                },
            }
        },
    }

    print(f"Creating index: {index_name}")
    resp = requests.put(
        f"{es_url}/{index_name}",
        json=index_config,
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    if resp.status_code in (200, 201):
        print("Index created successfully.")
        return True
    print(f"Create index failed: {resp.status_code} {resp.text}")
    return False


def create_search_templates(es_url=ES_URL):
    # 模板1：结构化优先（Java 传入拆解字段）
    structured_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"address.city_my": {"query": "{{city_my}}"}}},
                    {"match": {"address.region_my": {"query": "{{region_my}}"}}},
                    {"match": {"address.road_my": {"query": "{{road_my}}"}}},
                    {"term": {"address.house_number": "{{house_number}}"}},
                    {"match": {"address.building_my": {"query": "{{building_my}}"}}},
                ],
                "minimum_should_match": 1,
            }
        },
        "sort": [{"_score": "desc"}],
        "size": "{{size}}",
    }

    # 模板2：全文兜底
    fallback_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"search.full_my": {"query": "{{keyword}}"}}},
                    {"match": {"search.full_en": {"query": "{{keyword}}"}}},
                    {"match": {"search.full_zh": {"query": "{{keyword}}"}}},
                    {"match": {"names.name_my.ngram": {"query": "{{keyword}}"}}},
                    {"match": {"names.name_en.ngram": {"query": "{{keyword}}"}}},
                ],
                "minimum_should_match": 1,
            }
        },
        "sort": [{"_score": "desc"}],
        "size": "{{size}}",
    }

    # 模板3：结构化 + nested 地址组件补偿
    universal_query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "nested": {
                            "path": "address_parts",
                            "query": {
                                "function_score": {
                                    "query": {
                                        "match": {
                                            "address_parts.name.name_my": {
                                                "query": "{{keyword}}"
                                            }
                                        }
                                    },
                                    "functions": [
                                        {
                                            "script_score": {
                                                "script": {
                                                    "source": "Math.pow(2, doc['address_parts.rank'].value / 5.0)"
                                                }
                                            }
                                        }
                                    ],
                                    "boost_mode": "multiply",
                                }
                            },
                            "score_mode": "avg",
                        }
                    },
                    {"match": {"search.full_my": {"query": "{{keyword}}"}}},
                    {"match": {"names.name_my.ngram": {"query": "{{keyword}}"}}},
                ],
                "minimum_should_match": 1,
            }
        },
        "sort": [{"_score": "desc"}],
        "size": "{{size}}",
    }

    templates = [
        ("address_structured_v2", structured_query),
        ("address_fallback_v2", fallback_query),
        ("address_universal_v2", universal_query),
    ]

    for name, body in templates:
        payload = {"script": {"lang": "mustache", "source": json.dumps(body)}}
        print(f"Creating template: {name}")
        resp = requests.post(
            f"{es_url}/_scripts/{name}",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code not in (200, 201):
            print(f"Template create failed: {name}, {resp.status_code} {resp.text}")
            return False
    return True


def main():
    wait_for_elasticsearch()
    if not create_index():
        return 1
    if not create_search_templates():
        return 1
    print("Init ES V2 done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
