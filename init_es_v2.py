#!/usr/bin/env python3
import argparse
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ES_URL = "http://elasticsearch:9200"
INDEX_NAME = "address_places"


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


def get_mapping_properties(es_url=ES_URL, index_name=INDEX_NAME):
    try:
        resp = requests.get(f"{es_url}/{index_name}/_mapping", timeout=15)
        if resp.status_code != 200:
            return {}
        root = resp.json().get(index_name, {})
        return root.get("mappings", {}).get("properties", {})
    except requests.exceptions.RequestException:
        return {}


def mapping_is_expected(es_url=ES_URL, index_name=INDEX_NAME):
    props = get_mapping_properties(es_url, index_name)
    if not props:
        return False, "empty mapping"

    centroid_type = props.get("centroid", {}).get("type")
    address_parts_type = props.get("address_parts", {}).get("type")

    errors = []
    if centroid_type != "geo_point":
        errors.append(f"centroid.type expected geo_point, got {centroid_type}")
    if address_parts_type != "nested":
        errors.append(f"address_parts.type expected nested, got {address_parts_type}")

    if errors:
        return False, "; ".join(errors)
    return True, "ok"


def delete_index(es_url=ES_URL, index_name=INDEX_NAME):
    resp = requests.delete(f"{es_url}/{index_name}", timeout=30)
    if resp.status_code in (200, 202, 404):
        return True
    print(f"Delete index failed: {resp.status_code} {resp.text}")
    return False


def create_index(es_url=ES_URL, index_name=INDEX_NAME, force_recreate=False):
    try:
        resp = requests.head(f"{es_url}/{index_name}", timeout=10)
        if resp.status_code == 200:
            if force_recreate:
                print(f"Index '{index_name}' exists, deleting because --force-recreate is enabled...")
                if not delete_index(es_url, index_name):
                    return False
            else:
                ok, reason = mapping_is_expected(es_url, index_name)
                if ok:
                    print(f"Index '{index_name}' already exists and mapping looks correct, skip create.")
                    return True
                print(f"Index '{index_name}' already exists but mapping is not expected: {reason}")
                print("Hint: rerun with --force-recreate after stopping Logstash writers.")
                return False
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


def parse_args():
    parser = argparse.ArgumentParser(description="Initialize Elasticsearch index/mapping for map search.")
    parser.add_argument("--es-url", default=ES_URL, help="Elasticsearch base url, e.g. http://localhost:9200")
    parser.add_argument("--index", default=INDEX_NAME, help="Target index name")
    parser.add_argument("--force-recreate", action="store_true", help="Delete existing index before create")
    return parser.parse_args()


def main():
    args = parse_args()
    wait_for_elasticsearch(args.es_url)
    if not create_index(args.es_url, args.index, args.force_recreate):
        return 1
    ok, reason = mapping_is_expected(args.es_url, args.index)
    if not ok:
        print(f"WARNING: mapping validation failed after create: {reason}")
        return 1
    print("Init ES V2 done. Mapping validation passed. (_scripts are managed separately)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
