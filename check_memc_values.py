# Utility script to check if a value was set correctly to memcached
import memcache

addrs = ['127.0.0.1:33013', '127.0.0.1:33014', '127.0.0.1:33015', '127.0.0.1:33016']

# for addr in addrs:
#     memc = memcache.Client([addr, ], debug=1)
#     memc.flush_all()

# key = "idfa:1rfw452y52g2gq4g"
# key = "idfa:2rfw452y52g2gq4g"
# key = "idfa:3rfw452y52g2gq4g"
# key = "idfa:4rfw452y52g2gq4g"
# key = "idfa:5rfw452y52g2gq4g"
# key = "gaid:6rfw452y52g2gq4g"
# key = "gaid:7rfw452y52g2gq4g"
# key = "gaid:8rfw452y52g2gq4g"
# key = "gaid:9rfw452y52g2gq4g"
key = "idfa:659d72082e52be9719d33f33d69b568f"
# key = "idfa:e7e1a50c0ec2747ca56cd9e1558c0d7c"

if __name__ == "__main__":
    memc = memcache.Client(['127.0.0.1:33013'], debug=1)
    # memc.set(key, b'Blah')
    val = memc.get(key)  # depends on if flushed above!
    print(val)
