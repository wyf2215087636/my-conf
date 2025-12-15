#!/usr/bin/env python3
import json
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def wait_for_elasticsearch(url="http://elasticsearch:9200", timeout=300):
    """等待 Elasticsearch 启动并可用"""
    print("Waiting for Elasticsearch to be ready...")
    
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = session.get(f"{url}/_cluster/health", 
                                 params={"wait_for_status": "yellow", "timeout": "10s"},
                                 timeout=15)
            if response.status_code == 200:
                print("Elasticsearch is up - applying settings, mappings, and scripts")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print("Waiting for Elasticsearch to be ready...")
        time.sleep(5)
    
    raise Exception(f"Elasticsearch not ready after {timeout} seconds")

def create_index(es_url="http://elasticsearch:9200"):
    """创建索引配置"""
    
    # 首先检查索引是否已存在
    try:
        response = requests.head(f"{es_url}/address_places")
        if response.status_code == 200:
            print("Index 'address_places' already exists, skipping creation")
            return True
    except requests.exceptions.RequestException:
        pass
    
    index_config = {
          "settings": {
            "index.max_ngram_diff": 99,
            "index.mapping.total_fields.limit": 100000,
            "analysis": {
              "analyzer": {
                "myanmar_ngram": {
                  "type": "custom",
                  "tokenizer": "standard",
                  "filter": ["lowercase", "my_ngram"]
                }
              },
              "filter": {
                "my_ngram": {
                  "type": "ngram",
                  "min_gram": 4,
                  "max_gram": 9
                }
              }
            }
          },
          "mappings": {
            "properties": {
              "names": {
                "properties": {
                  "name": {
                    "type": "text",
                    "analyzer": "myanmar_kytea_analyzer",
                    "search_analyzer": "myanmar_kytea_analyzer",
                    "fields": {
                      "ngram": {
                        "type": "text",
                        "analyzer": "myanmar_ngram",
                        "search_analyzer": "myanmar_ngram"
                      }
                    }
                  },
                  "name:my": {
                    "type": "text",
                    "analyzer": "myanmar_kytea_analyzer",
                    "search_analyzer": "myanmar_kytea_analyzer",
                    "fields": {
                      "ngram": {
                        "type": "text",
                        "analyzer": "myanmar_ngram",
                        "search_analyzer": "myanmar_ngram"
                      }
                    }
                  },
                  "name:en": {
                    "type": "text"
                  }
                }
              },
              "address_parts": {
                "type": "nested",
                "properties": {
                  "name": {
                    "properties": {
                      "name:my": {
                        "type": "text",
                        "analyzer": "myanmar_kytea_analyzer",
                        "search_analyzer": "myanmar_kytea_analyzer",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          },
                          "ngram": {
                            "type": "text",
                            "analyzer": "myanmar_ngram",
                            "search_analyzer": "myanmar_ngram"
                          }
                        }
                      }
                    }
                  },
                  "rank": {
                    "type": "integer"
                  }
                }
              },
              "centroid": {
                "properties": {
                  "coordinates": {
                    "type": "geo_point"
                  },
                  "type": {
                    "type": "keyword"
                  }
                }
              }
            }
          }
        }
    
    print("Creating/Updating address_places index...")
    response = requests.put(f"{es_url}/address_places", 
                           json=index_config,
                           headers={"Content-Type": "application/json"})
    
    if response.status_code in [200, 201]:
        print("Index created/updated successfully")
    elif response.status_code == 400 and "already exists" in response.text:
        print("Index already exists, skipping creation")
    else:
        print(f"Error creating index: {response.status_code} - {response.text}")
        return False
    
    return True

def create_search_templates(es_url="http://elasticsearch:9200"):
    """创建搜索模板"""
    
    # 地址搜索模板
    address_places_search_query = {
        "query": {
            "nested": {
                "path": "address_parts",
                "query": {
                    "function_score": {
                        "query": {
                            "match": {
                                "address_parts.name.name:my": {
                                    "query": "{{keyword}}"
                                }
                            }
                        },
                        "functions": [{
                            "script_score": {
                                "script": {
                                    "source": "Math.pow(2, doc['address_parts.rank'].value / 5)"
                                }
                            }
                        }],
                        "boost_mode": "multiply"
                    }
                },
                "score_mode": "avg",
                "inner_hits": {
                    "size": 3
                }
            }
        },
        "sort": ["_score"],
        "size": "{{size}}"
    }
    
    address_places_search_tmpl = {
        "script": {
            "lang": "mustache",
            "source": json.dumps(address_places_search_query)
        }
    }
    
    # 名称搜索模板
    name_search_query = {
        "query": {
            "multi_match": {
                "fields": ["names.name:my.ngram", "names.name.ngram"],
                "query": "{{keyword}}"
            }
        },
        "size": "{{size}}"
    }
    
    name_search_tmpl = {
        "script": {
            "lang": "mustache",
            "source": json.dumps(name_search_query)
        }
    }
    
    # 通用名称地址搜索模板
    universal_search_query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "fields": ["names.name:my.ngram", "names.name.ngram"],
                            "query": "{{keyword}}"
                        }
                    },
                    {
                        "nested": {
                            "path": "address_parts",
                            "query": {
                                "function_score": {
                                    "query": {
                                        "match": {
                                            "address_parts.name.name:my": {
                                                "query": "{{keyword}}"
                                            }
                                        }
                                    },
                                    "functions": [{
                                        "script_score": {
                                            "script": {
                                                "source": "Math.pow(2, doc['address_parts.rank'].value / 5)"
                                            }
                                        }
                                    }],
                                    "boost_mode": "multiply"
                                }
                            },
                            "score_mode": "avg",
                            "inner_hits": {
                                "size": 3
                            }
                        }
                    }
                ]
            }
        },
        "sort": ["_score"],
        "size": "{{size}}"
    }
    
    universal_search_tmpl = {
        "script": {
            "lang": "mustache",
            "source": json.dumps(universal_search_query)
        }
    }
    
    # 创建模板
    templates = [
        ("address_places_search", address_places_search_tmpl),
        ("name_search", name_search_tmpl),
        ("universal_name_address_search", universal_search_tmpl)
    ]
    
    for template_name, template_body in templates:
        print(f"Creating/Updating _scripts/{template_name}...")
        response = requests.post(f"{es_url}/_scripts/{template_name}",
                               json=template_body,
                               headers={"Content-Type": "application/json"})
        
        if response.status_code in [200, 201]:
            print(f"Template {template_name} created/updated successfully")
        else:
            print(f"Error creating template {template_name}: {response.status_code} - {response.text}")
            return False
    
    return True

def main():
    """主函数"""
    try:
        # 等待 Elasticsearch 启动
        wait_for_elasticsearch()
        
        # 创建索引
        if not create_index():
            return 1
        
        # 创建搜索模板
        if not create_search_templates():
            return 1
        
        print("Elasticsearch initialization script finished successfully.")
        return 0
        
    except Exception as e:
        print(f"Error during initialization: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 
