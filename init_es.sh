#!/bin/bash

# 等待 Elasticsearch 启动并可用
# The script will be run from a different container, so it targets 'elasticsearch' hostname
until curl -s http://elasticsearch:9200/_cluster/health?wait_for_status=yellow&timeout=10s; do
    echo "Waiting for Elasticsearch to be ready..."
    sleep 5
done

echo "Elasticsearch is up - applying settings, mappings, and scripts"

# 1. 定义索引配置
INDEX_CONFIG='{
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
}'

# 2. 创建索引
echo "Creating/Updating address_places index..."
curl -X PUT "http://elasticsearch:9200/address_places" \
  -H 'Content-Type: application/json' \
  -d "$INDEX_CONFIG"
echo ""

# 3. 定义模板内容（source 必须是字符串！sort/size 在 query 外部，size 不加引号）
ADDRESS_PLACES_SEARCH_TMPL='{
  "script": {
    "lang": "mustache",
    "source": "{\"query\":{\"nested\":{\"path\":\"address_parts\",\"query\":{\"function_score\":{\"query\":{\"match\":{\"address_parts.name.name:my\":{\"query\":\"{{keyword}}\"}}},\"functions\":[{\"script_score\":{\"script\":{\"source\":\"Math.pow(2, doc['address_parts.rank'].value / 5)\"}}}],\"boost_mode\":\"multiply\"}},\"score_mode\":\"avg\",\"inner_hits\":{\"size\":3}}},\"sort\":[\"_score\"],\"size\":{{size}}}"
  }
}'

NAME_SEARCH_TMPL='{
  "script": {
    "lang": "mustache",
    "source": "{\"query\":{\"multi_match\":{\"fields\":[\"names.name:my.ngram\",\"names.name.ngram\"],\"query\":\"{{keyword}}\"}},\"size\":{{size}}}"
  }
}'

UNIVERSAL_NAME_ADDRESS_SEARCH_TMPL='{
  "script": {
    "lang": "mustache",
    "source": "{\"query\":{\"bool\":{\"should\":[{\"multi_match\":{\"fields\":[\"names.name:my.ngram\",\"names.name.ngram\"],\"query\":\"{{keyword}}\"}},{\"nested\":{\"path\":\"address_parts\",\"query\":{\"function_score\":{\"query\":{\"match\":{\"address_parts.name.name:my\":{\"query\":\"{{keyword}}\"}}},\"functions\":[{\"script_score\":{\"script\":{\"source\":\"Math.pow(2, doc['address_parts.rank'].value / 5)\"}}}],\"boost_mode\":\"multiply\"}},\"score_mode\":\"avg\",\"inner_hits\":{\"size\":3}}}]},\"sort\":[\"_score\"],\"size\":{{size}}}"
  }
}'

# 4. 创建/更新模板
echo "Creating/Updating _scripts/address_places_search..."
curl -X POST "http://elasticsearch:9200/_scripts/address_places_search" \
  -H 'Content-Type: application/json' \
  -d "$ADDRESS_PLACES_SEARCH_TMPL"
echo ""

echo "Creating/Updating _scripts/name_search..."
curl -X POST "http://elasticsearch:9200/_scripts/name_search" \
  -H 'Content-Type: application/json' \
  -d "$NAME_SEARCH_TMPL"
echo ""

echo "Creating/Updating _scripts/universal_name_address_search..."
curl -X POST "http://elasticsearch:9200/_scripts/universal_name_address_search" \
  -H 'Content-Type: application/json' \
  -d "$UNIVERSAL_NAME_ADDRESS_SEARCH_TMPL"
echo ""

echo "Elasticsearch initialization script finished."
