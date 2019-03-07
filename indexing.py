from __future__ import print_function

from abc import ABC, abstractmethod
from io import StringIO

from luigi import Parameter, WrapperTask, Task, LocalTarget
from luigi.contrib.external_program import ExternalProgramTask
from luigi.contrib.hdfs import HdfsTarget, HdfsClientApache1
from luigi.contrib.ssh import RemoteContext
from plumbum.path.utils import move


class ArquivoIndexingExternalTask(ExternalProgramTask, ABC):
    data_collections_folder = Parameter(default="/data/collections")
    document_server = Parameter(default="p64.arquivo.pt")
    lucene_jar = Parameter(default="/opt/searcher/scripts/lib/pwalucene-1.0.0-SNAPSHOT.jar")
    hadoop_bin = Parameter(default="/opt/searcher/hadoop/bin/hadoop")
    collection_name = Parameter(default='dummy')
    data_folder = Parameter(default='/data')
    hadoop_jar = Parameter(default="/opt/searcher/scripts/nutchwax-job-0.11.0-SNAPSHOT.jar")
    # TODO this can be configured within Luigi conf file
    hadoop_bin = Parameter(default="/opt/searcher/hadoop/bin/hadoop")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.checkpoint_file = self.set_checkpoint()

    @abstractmethod
    def set_checkpoint(self):
        pass

    def on_success(self):
        with open(self.checkpoint_file, mode='w'):
            pass
        super().on_success()

    def complete(self):
        return LocalTarget(self.checkpoint_file).exists()


class GenerateArcList(Task):
    """
    Connects to a Document Server and builds a list of (W)ARC that will be indexed. Keys need to be exchanged.
    """
    data_collections_folder = Parameter(default="/data/collections")
    collection_name = Parameter(default="dummy")
    document_server = Parameter(default="p64.arquivo.pt")
    data_folder = Parameter(default='/data')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.checkpoint_file = self.set_checkpoint()

    def complete(self):
        return LocalTarget(self.checkpoint_file).exists()

    def on_success(self):
        with open(self.checkpoint_file, mode='w'):
            pass
        super().on_success()

    def set_checkpoint(self):
        return "{}/{}.generatearc.checkpoint".format(self.data_folder, self.collection_name)

    def output(self):
        input_path_name = 'inputs_{}'.format(self.collection_name)
        hdfs_client = HdfsClientApache1()
        hdfs_client.mkdir(input_path_name)

        return HdfsTarget('inputs_{}/arcs.txt'.format(self.collection_name))

    def run(self):
        remote = RemoteContext(self.document_server)

        with self.output().open('w') as outfile:
            stream = StringIO(remote.check_output(["cd {} && find ./{} -type f -regex \".*[w]*arc\.gz\"".format(
                self.data_collections_folder, self.collection_name)]).decode())

            for line in stream:
                outfile.write(line.replace('./', 'http://{}:8080/browser/files/'.format(self.document_server)))


class ImportCollectionHadoop(ArquivoIndexingExternalTask):

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar, "importwarc",
                          "inputs_{}".format(self.collection_name),
                          "outputs_{}".format(self.collection_name), self.collection_name]
        return hadoop_arglist

    def requires(self):
        return GenerateArcList(data_collections_folder=self.data_collections_folder,
                               collection_name=self.collection_name, document_server=self.document_server)

    def on_success(self):
        with open(self.checkpoint_file, mode='w'):
            pass
        super().on_success()

    def complete(self):
        return LocalTarget(self.checkpoint_file).exists()

    def set_checkpoint(self):
        return "{}/{}.import.checkpoint".format(self.data_folder, self.collection_name)


class UpdateCrawlDBHadoop(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.update.checkpoint".format(self.data_folder, self.collection_name)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar, "update", "outputs_{}".format(self.collection_name)]
        return hadoop_arglist

    def requires(self):
        return ImportCollectionHadoop(collection_name=self.collection_name, data_folder=self.data_folder,
                                      hadoop_jar=self.hadoop_jar, document_server=self.document_server)


class CreateLinkDB(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.invert.checkpoint".format(self.data_folder, self.collection_name)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar, "invert", "outputs_{}".format(self.collection_name)]
        return hadoop_arglist

    def requires(self):
        return UpdateCrawlDBHadoop(collection_name=self.collection_name, data_folder=self.data_folder,
                                   hadoop_jar=self.hadoop_jar, document_server=self.document_server)


class IndexCollection(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.index.checkpoint".format(self.data_folder, self.collection_name)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar, "index", "outputs_{}".format(self.collection_name)]
        return hadoop_arglist

    def requires(self):
        return CreateLinkDB(collection_name=self.collection_name, data_folder=self.data_folder,
                            hadoop_jar=self.hadoop_jar, document_server=self.document_server)


class MergeIndexSegments(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.merge.checkpoint".format(self.data_folder, self.collection_name)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar, "merge", "outputs_{}".format(self.collection_name)]
        return hadoop_arglist

    def requires(self):
        return IndexCollection(collection_name=self.collection_name, data_folder=self.data_folder,
                               hadoop_jar=self.hadoop_jar, document_server=self.document_server)


class CopyIndexesFromHdfs(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.copylocal.checkpoint".format(self.data_folder, self.collection_name)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'dfs', '-copyToLocal', "outputs_{}".format(self.collection_name),
                          self.data_folder]
        return hadoop_arglist

    def requires(self):
        return MergeIndexSegments(collection_name=self.collection_name, data_folder=self.data_folder,
                                  hadoop_jar=self.hadoop_jar, document_server=self.document_server)


# Sort a Nutch index by page score.
# Higher scoring documents are assigned smaller document numbers.
class SortIndexes(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.sort_indexes.checkpoint".format(self.data_folder, self.collection_name)

    def requires(self):
        return CopyIndexesFromHdfs(collection_name=self.collection_name, data_folder=self.data_folder,
                                   hadoop_jar=self.hadoop_jar, document_server=self.document_server)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar,
                          "class", "org.apache.nutch.indexer.IndexSorterArquivoWeb",
                          "{}/outputs_{}".format(self.data_folder, self.collection_name)]
        return hadoop_arglist

    def on_success(self):
        super().on_success()
        move("{}/outputs_{}/index".format(self.data_folder, self.collection_name),
             "{}/outputs_{}/index-orig".format(self.data_folder, self.collection_name))
        move("{}/outputs_{}/index-sorted".format(self.data_folder, self.collection_name),
             "{}/outputs_{}/index".format(self.data_folder, self.collection_name))


class PruneIndexes(ArquivoIndexingExternalTask):

    def requires(self):
        return SortIndexes(collection_name=self.collection_name, data_folder=self.data_folder,
                           hadoop_jar=self.hadoop_jar, lucene_jar=self.lucene_jar, hadoop_bin=self.hadoop_bin,
                           document_server=self.document_server)

    def program_args(self):
        # org.apache.lucene.pruning.PruningTool -impl arquivo -in $INDEX_DIR -out $INDEX_DIR_OUT -del pagerank:pPsv,outlinks:pPsv -t 1
        task_arglist = ["java", "-Xmx5000m", "-classpath", self.lucene_jar, "org.apache.lucene.pruning.PruningTool",
                        "-impl", "arquivo", "-in", "{}/outputs_{}/index".format(self.data_folder, self.collection_name),
                        "-out",
                        "{}/outputs_{}/index-prunning".format(self.data_folder, self.collection_name),
                        "-del", "pagerank:pPsv,outlinks:pPsv", "-t", "1"]
        return task_arglist

    def set_checkpoint(self):
        return "{}/{}.prunning.checkpoint".format(self.data_folder, self.collection_name)

    def on_success(self):
        super().on_success()
        move("{}/{}/index".format(self.data_folder, self.collection_name),
             "{}/{}/index-sorted".format(self.data_folder, self.collection_name))
        move("{}/{}/index-prunning".format(self.data_folder, self.collection_name),
             "{}/{}/index".format(self.data_folder, self.collection_name))


class StartIndexCollection(WrapperTask):
    collection_name = Parameter(default='dummy')
    data_folder = Parameter(default='/data')
    hadoop_jar = Parameter(default="/opt/searcher/scripts/nutchwax-job-0.11.0-SNAPSHOT.jar")
    hadoop_bin = Parameter(default="/opt/searcher/hadoop/bin/hadoop")
    lucene_jar = Parameter(default="/opt/searcher/scripts/lib/pwalucene-1.0.0-SNAPSHOT.jar")
    document_server = Parameter(default="p64.arquivo.pt")

    def complete(self):
        return False

    def requires(self):
        return PruneIndexes(collection_name=self.collection_name, data_folder=self.data_folder,
                            hadoop_jar=self.hadoop_jar, lucene_jar=self.lucene_jar, hadoop_bin=self.hadoop_bin,
                            document_server=self.document_server)
