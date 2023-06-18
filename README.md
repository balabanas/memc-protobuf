# memc-protobuf
Memcached Concurrent Data Load

## Goal
The goal is to refactor the existing script which loads data from .gzipped files into several instances of **memcached** (`memc_load_serial.py`), so to implement concurrent inserting instead of serial one. There are both I/O- and CPU-bounded tasks there which might be good for concurrent performance:
* I/O-bounded insert operation, which require network interaction
* CPU-bound conversion operation by packing each data record into binary code with **protobuf** utility.

## Decisions made
1. We open 4 connections with corresponding 4 instances of memcached (one per device type)
2. We use `ProcessPoolExecutor` and `ThreadPoolExecutor` from `concurrent.futures`.
3. We start with running 3 processes for parallel processing of data files: each file in a directory is assigned in turn to an available process.
4. Within each process, we read lines in batches of 20 000 records. 
5. Then each batch is splitted on 4, by device type. At this stage lines are packed into protobuf object.
6. Then we assign each batch split to one of 4 threads, each is responsible for .set_multi bunch of records with similar batch_type.
7. The idea to read in batches is to not create too large dictionaries with futures in memory, and made a process more responsive (we have a counter of batches there).
9. Upon finishing processing each batch we count number of successfully processed (inserted) records, and are ready to process another batch.

### Productivity boosters:
* Persistant connections
* Several processes to process files in parallel
* Batch inserting by .set_multi
* Several threads

### See also:
* Version with multiprocessing and multithreading, but with no persistand connection and .set_multi: https://github.com/balabanas/memc-protobuf/tree/8ce749771d1683c12e2a2cea43a4d98454e09042

## Run instructions
1. Start memcached containers: `docker compose up`
2. Make sure the data to load is in appropriate directory, and named appropriately (default is `/data/appsinstalled/*.tsv.gz`), but you may alter parameters in `op.add_option` section of the `memc_load.py`. NB! If you whant to use data from `data/appinstalled directory` of the source code, set the parameter `pattern` to `data/appsinstalled/*.tsv.gz`: the first slash is omitted.
3. Run `main` in `memc_load.py`: `python memc_load.py`

## Run tests
1. Start memcached containers: `docker compose up`
2. Run python test.py

## Potentially better ways to load data into memory faster
The goal of this example was to demonstrate multiprocessing/multithreading facilities in concurrent load. If our ultimate goal was to minimize load times, a couple of other strategies could be potentially useful in combination with multiprocessing and multithreading:
1. Do not wait the response from the server after each set: `memc.set(key, packed, noreply=True)`. It is about 30-40% faster than waiting for the response, according to the preliminary tests. This will not give a chance to count number of successful inserts directly, but if we are fine with a small margin of data being lost, we can estimate the proportion of successes statistically, by storing random subset of the key/values in a separate dictionary, and trying to read them back after the end of the job. This will give us a point estimation of the proportion of correct inserts, and we can easily calculate a confidence interval as well.
2. Using batch load with `memc.set_multi()`, not tested, but likely provides the huge gain in productivity.

## Some resources and instructions
* The instruction to install Google's protocol buffers on Windows: `https://www.geeksforgeeks.org/how-to-install-protocol-buffers-on-windows/`. ProtoC compiler version: `protoc-23.2-win64.zip`
* Make appinstalled_pb2.py from appsinstalled.proto file with: `protoc  --python_out=. ./appsinstalled.proto`
* In the source, `_appinstalled_pb2.py` corresponds to an older version of a protoc compiler (came with a task), but `appinstalled_pb2.py` is created with a newer version of the compiler.