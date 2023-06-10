import memcache


# key = "idfa:1rfw452y52g2gq4g"
# key = "idfa:2rfw452y52g2gq4g"
# key = "idfa:3rfw452y52g2gq4g"
# key = "idfa:4rfw452y52g2gq4g"
# key = "idfa:5rfw452y52g2gq4g"
# key = "gaid:6rfw452y52g2gq4g"
# key = "gaid:7rfw452y52g2gq4g"
# key = "gaid:8rfw452y52g2gq4g"
# key = "gaid:9rfw452y52g2gq4g"
key = "gaid:10fw452y52g2gq4g"

if __name__ == "__main__":
    memc = memcache.Client(['127.0.0.1:33013'])
    val = memc.get(key)
    print(val)