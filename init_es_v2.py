#!/usr/bin/env python3
import argparse
import json
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
    address_props = props.get("address", {}).get("properties", {})

    errors = []
    if centroid_type != "geo_point":
        errors.append(f"centroid.type expected geo_point, got {centroid_type}")
    if address_parts_type != "nested":
        errors.append(f"address_parts.type expected nested, got {address_parts_type}")
    for field in ["district_my", "township_my", "ward_my", "crossroads_my"]:
        if field not in address_props:
            errors.append(f"address.{field} missing")

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
                        "district_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "district_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "district_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "township_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "township_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "township_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "ward_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "ward_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "ward_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "road_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "road_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "road_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "crossroads_my": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "crossroads_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "crossroads_zh": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
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
    # Structured template: strong one-to-one matching for region/district/township/ward/street/house/poi/crossroads.
    structured_query = {
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "should": [
                            {"term": {"address.house_number": {"value": "{{house_number}}", "boost": 12}}},
                            {"match_phrase": {"address.road_my": {"query": "{{road_my}}", "boost": 12}}},
                            {"match_phrase": {"address.road_en": {"query": "{{road_en}}", "boost": 10}}},
                            {"match_phrase": {"address.crossroads_my": {"query": "{{crossroads_my}}", "boost": 12}}},
                            {"match_phrase": {"address.crossroads_en": {"query": "{{crossroads_en}}", "boost": 10}}},
                            {"match_phrase": {"address.building_my": {"query": "{{building_my}}", "boost": 16}}},
                            {"match_phrase": {"address.building_en": {"query": "{{building_en}}", "boost": 13}}},
                            {"match_phrase": {"names.name_my": {"query": "{{poi_my}}", "boost": 18}}},
                            {"match_phrase": {"names.name_en": {"query": "{{poi_en}}", "boost": 14}}},
                            {"match": {"address.city_my": {"query": "{{city_my}}", "boost": 2}}},
                            {"match": {"address.city_en": {"query": "{{city_en}}", "boost": 2}}},
                            {"match": {"address.region_my": {"query": "{{region_my}}", "boost": 3}}},
                            {"match": {"address.region_en": {"query": "{{region_en}}", "boost": 3}}},
                            {"term": {"address.district_my.keyword": {"value": "{{district_my}}", "boost": 11}}},
                            {"term": {"address.township_my.keyword": {"value": "{{township_my}}", "boost": 12}}},
                            {"term": {"address.ward_my.keyword": {"value": "{{ward_my}}", "boost": 13}}},
                            {"term": {"address.crossroads_my.keyword": {"value": "{{crossroads_my}}", "boost": 12}}},
                            {"match": {"address.district_my": {"query": "{{district_my}}", "boost": 8}}},
                            {"match": {"address.district_en": {"query": "{{district_en}}", "boost": 7}}},
                            {"match": {"address.township_my": {"query": "{{township_my}}", "boost": 9}}},
                            {"match": {"address.township_en": {"query": "{{township_en}}", "boost": 8}}},
                            {"match": {"address.ward_my": {"query": "{{ward_my}}", "boost": 10}}},
                            {"match": {"address.ward_en": {"query": "{{ward_en}}", "boost": 9}}}
                        ],
                        "minimum_should_match": 1
                    }
                },
                "functions": [
                    {
                        "filter": {
                            "bool": {
                                "must": [
                                    {"term": {"address.house_number": "{{house_number}}"}},
                                    {"match_phrase": {"address.road_my": {"query": "{{road_my}}"}}}
                                ]
                            }
                        },
                        "weight": 6
                    },
                    {
                        "filter": {
                            "bool": {
                                "must": [
                                    {"match_phrase": {"names.name_my": {"query": "{{poi_my}}"}}},
                                    {"match_phrase": {"address.road_my": {"query": "{{road_my}}"}}}
                                ]
                            }
                        },
                        "weight": 8
                    },
                    {
                        "filter": {
                            "bool": {
                                "must": [
                                    {"match_phrase": {"address.road_my": {"query": "{{road_my}}"}}},
                                    {"match_phrase": {"address.crossroads_my": {"query": "{{crossroads_my}}"}}}
                                ]
                            }
                        },
                        "weight": 9
                    },
                    {
                        "filter": {
                            "bool": {
                                "must": [
                                    {"match": {"address.district_my": {"query": "{{district_my}}"}}},
                                    {"match": {"address.township_my": {"query": "{{township_my}}"}}},
                                    {"match": {"address.ward_my": {"query": "{{ward_my}}"}}}
                                ]
                            }
                        },
                        "weight": 7
                    }
                ],
                "score_mode": "sum",
                "boost_mode": "sum"
            }
        },
        "sort": [{"_score": "desc"}, {"importance": "desc"}],
        "size": "{{size}}"
    }

    fallback_query = {
        "query": {
            "bool": {
                "should": [
                    {"match_phrase": {"search.full_my": {"query": "{{keyword}}", "boost": 8}}},
                    {"match_phrase": {"search.full_en": {"query": "{{keyword}}", "boost": 6}}},
                    {"match_phrase": {"search.full_zh": {"query": "{{keyword}}", "boost": 5}}},
                    {
                        "multi_match": {
                            "query": "{{keyword}}",
                            "type": "best_fields",
                            "fields": [
                                "search.full_my^4",
                                "search.full_en^3",
                                "search.full_zh^2",
                                "names.name_my.ngram^4",
                                "names.name_en.ngram^3",
                                "names.name_zh.ngram^2",
                                "names.name_my^5",
                                "names.name_en^4",
                                "names.name_zh^3",
                                "address.crossroads_my^5",
                                "address.crossroads_en^4",
                                "address.ward_my^5",
                                "address.township_my^4",
                                "address.district_my^3"
                            ]
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        },
        "sort": [{"_score": "desc"}, {"importance": "desc"}],
        "size": "{{size}}"
    }

    road_only_query = {
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "bool": {
                                    "should": [
                                        {"match_phrase": {"address.road_my": {"query": "{{road_my}}", "boost": 10}}},
                                        {"match_phrase": {"address.road_en": {"query": "{{road_en}}", "boost": 9}}},
                                        {"match_phrase": {"address.crossroads_my": {"query": "{{crossroads_my}}", "boost": 10}}},
                                        {"match_phrase": {"address.crossroads_en": {"query": "{{crossroads_en}}", "boost": 9}}},
                                        {"match_phrase": {"search.full_my": {"query": "{{road_my}}", "boost": 8}}},
                                        {"match_phrase": {"search.full_en": {"query": "{{road_en}}", "boost": 7}}},
                                        {"match_phrase": {"names.name_my": {"query": "{{road_my}}", "boost": 8}}},
                                        {"match_phrase": {"names.name_en": {"query": "{{road_en}}", "boost": 6}}}
                                    ],
                                    "minimum_should_match": 1
                                }
                            }
                        ],
                        "should": [
                            {"match": {"address.city_my": {"query": "{{city_my}}", "boost": 2}}},
                            {"match": {"address.city_en": {"query": "{{city_en}}", "boost": 2}}},
                            {"match": {"address.region_my": {"query": "{{region_my}}", "boost": 3}}},
                            {"match": {"address.region_en": {"query": "{{region_en}}", "boost": 3}}},
                            {"term": {"address.district_my.keyword": {"value": "{{district_my}}", "boost": 9}}},
                            {"term": {"address.township_my.keyword": {"value": "{{township_my}}", "boost": 10}}},
                            {"term": {"address.ward_my.keyword": {"value": "{{ward_my}}", "boost": 11}}},
                            {"term": {"address.crossroads_my.keyword": {"value": "{{crossroads_my}}", "boost": 10}}},
                            {"match": {"address.district_my": {"query": "{{district_my}}", "boost": 6}}},
                            {"match": {"address.district_en": {"query": "{{district_en}}", "boost": 5}}},
                            {"match": {"address.township_my": {"query": "{{township_my}}", "boost": 7}}},
                            {"match": {"address.township_en": {"query": "{{township_en}}", "boost": 6}}},
                            {"match": {"address.ward_my": {"query": "{{ward_my}}", "boost": 8}}},
                            {"match": {"address.ward_en": {"query": "{{ward_en}}", "boost": 7}}}
                        ]
                    }
                },
                "functions": [
                    {
                        "filter": {
                            "bool": {
                                "should": [
                                    {"match_phrase": {"address.road_my": {"query": "{{road_my}}"}}},
                                    {"match_phrase": {"address.road_en": {"query": "{{road_en}}"}}},
                                    {"match_phrase": {"address.crossroads_my": {"query": "{{crossroads_my}}"}}},
                                    {"match_phrase": {"address.crossroads_en": {"query": "{{crossroads_en}}"}}}
                                ],
                                "minimum_should_match": 1
                            }
                        },
                        "weight": 3
                    }
                ],
                "score_mode": "sum",
                "boost_mode": "sum"
            }
        },
        "sort": [{"_score": "desc"}, {"importance": "desc"}],
        "size": "{{size}}"
    }

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
                                        "bool": {
                                            "should": [
                                                {"match": {"address_parts.name.name_my": {"query": "{{keyword}}"}}},
                                                {"match": {"address_parts.name.name:my": {"query": "{{keyword}}"}}}
                                            ],
                                            "minimum_should_match": 1
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
                                    "boost_mode": "multiply"
                                }
                            },
                            "score_mode": "avg"
                        }
                    },
                    {"match_phrase": {"search.full_my": {"query": "{{keyword}}", "boost": 8}}},
                    {"match_phrase": {"search.full_en": {"query": "{{keyword}}", "boost": 6}}},
                    {"match_phrase": {"search.full_zh": {"query": "{{keyword}}", "boost": 5}}},
                    {
                        "multi_match": {
                            "query": "{{keyword}}",
                            "type": "best_fields",
                            "fields": [
                                "names.name_my^6",
                                "names.name_en^4",
                                "names.name_zh^3",
                                "names.name_my.ngram^4",
                                "names.name_en.ngram^3",
                                "names.name_zh.ngram^2",
                                "address.road_my^5",
                                "address.road_en^4",
                                "address.crossroads_my^5",
                                "address.crossroads_en^4",
                                "address.ward_my^5",
                                "address.township_my^4",
                                "address.district_my^3",
                                "address.building_my^4",
                                "address.building_en^3",
                                "address.city_my^2",
                                "address.city_en^2",
                                "address.region_my^3",
                                "address.region_en^3"
                            ]
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        },
        "sort": [{"_score": "desc"}, {"importance": "desc"}],
        "size": "{{size}}"
    }

    templates = [
        ("address_structured_v2", structured_query),
        ("address_fallback_v2", fallback_query),
        ("address_road_only_v2", road_only_query),
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

def parse_args():
    parser = argparse.ArgumentParser(description="Initialize Elasticsearch mapping/templates for map search.")
    parser.add_argument("--es-url", default=ES_URL, help="Elasticsearch base url, e.g. http://localhost:9200")
    parser.add_argument("--index", default=INDEX_NAME, help="Target index name")
    parser.add_argument("--force-recreate", action="store_true", help="Delete existing index before create")
    return parser.parse_args()


def main():
    args = parse_args()
    wait_for_elasticsearch(args.es_url)
    if not create_index(args.es_url, args.index, args.force_recreate):
        return 1
    if not create_search_templates(args.es_url):
        return 1
    ok, reason = mapping_is_expected(args.es_url, args.index)
    if not ok:
        print(f"WARNING: mapping validation failed after create: {reason}")
        return 1
    print("Init ES V2 done. Mapping validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

