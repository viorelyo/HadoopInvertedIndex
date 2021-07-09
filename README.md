# HadoopInvertedIndex
:elephant: Inverted index for a book repository implemented using Hadoop Map-Reduce.

### Features
* The inverted index contains for each distinct unique word, a list of files containing the given word with its location within the file (line number). 
> Example: "masterpiece	(pg35473 | 1066) (pg4300 | 2970, 19224)"

### Build and Run steps
1. When running the application you should have a small cluster/cloud of at least two nodes build from VMs – eventually a larger cluster build from all your individual VMs.
2. A stopwords file `stopwords.txt` is required.
3. `hadoop/sbin/start-yarn.sh`
4. `hadoop/sbin/start-dfs.sh`
5. `jps`, `ssh node1 jps` - make sure all required processes are running on every cluster-node
6. `hdfs dfs -copyFromLocal input /` - `input` directory contains the book repository
7. Build the `InversedIndex.jar` using gradle
8. `hadoop jar InversedIndex.jar /input /output`
9. `hdfs dfs -copyToLocal /output/* .`
10. `stop-all.sh`

### Map-Reduce Implementation
* **Method 1** uses a `FilePreprocessor` for adding the line numbers in the book repository.
* **Method 2** uses the `LineJob` which computes the line numbers based on the offset.

### Contributors
* [cherryDevBomb](https://github.com/cherryDevBomb)
* [viorelyo](https://github.com/viorelyo)
* [AlexandruCiorba](https://github.com/AlexandruCiorba)
