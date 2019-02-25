from abc import ABC, abstractmethod

from luigi import ExternalTask, Parameter, WrapperTask, LocalTarget
from luigi.contrib.external_program import ExternalProgramTask
from luigi.contrib.hdfs import HdfsTarget
from plumbum.path.utils import move


class ArquivoIndexingExternalTask(ExternalProgramTask, ABC):
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


# TODO do a task to create this file
class ArcList(ExternalTask):
    arc_list = Parameter(default='inputs/arcs.txt')

    def complete(self):
        return True

    def output(self):
        return HdfsTarget(self.arc_list)


class ImportCollectionHadoop(ArquivoIndexingExternalTask):

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar, "importwarc",
                          "inputs_{}".format(self.collection_name),
                          "outputs_{}".format(self.collection_name), self.collection_name]
        return hadoop_arglist

    def requires(self):
        return ArcList(arc_list="inputs_{}/arcs.txt".format(self.collection_name))

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
                                      hadoop_jar=self.hadoop_jar)


class CreateLinkDB(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.invert.checkpoint".format(self.data_folder, self.collection_name)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar, "invert", "outputs_{}".format(self.collection_name)]
        return hadoop_arglist

    def requires(self):
        return UpdateCrawlDBHadoop(collection_name=self.collection_name, data_folder=self.data_folder,
                                   hadoop_jar=self.hadoop_jar)


class IndexCollection(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.index.checkpoint".format(self.data_folder, self.collection_name)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar, "index", "outputs_{}".format(self.collection_name)]
        return hadoop_arglist

    def requires(self):
        return CreateLinkDB(collection_name=self.collection_name, data_folder=self.data_folder,
                            hadoop_jar=self.hadoop_jar)


class MergeIndexSegments(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.merge.checkpoint".format(self.data_folder, self.collection_name)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'jar', self.hadoop_jar, "merge", "outputs_{}".format(self.collection_name)]
        return hadoop_arglist

    def requires(self):
        return IndexCollection(collection_name=self.collection_name, data_folder=self.data_folder,
                               hadoop_jar=self.hadoop_jar)


class CopyIndexesFromHdfs(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.copylocal.checkpoint".format(self.data_folder, self.collection_name)

    def program_args(self):
        hadoop_arglist = [self.hadoop_bin, 'dfs', '-copyToLocal', "outputs_{}".format(self.collection_name),
                          self.data_folder]
        return hadoop_arglist

    def requires(self):
        return MergeIndexSegments(collection_name=self.collection_name, data_folder=self.data_folder,
                                  hadoop_jar=self.hadoop_jar)


# Sort a Nutch index by page score.
# Higher scoring documents are assigned smaller document numbers.
class SortIndexes(ArquivoIndexingExternalTask):

    def set_checkpoint(self):
        return "{}/{}.sort_indexes.checkpoint".format(self.data_folder, self.collection_name)

    def requires(self):
        return CopyIndexesFromHdfs(collection_name=self.collection_name, data_folder=self.data_folder,
                                   hadoop_jar=self.hadoop_jar)

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
                           hadoop_jar=self.hadoop_jar)

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

    def complete(self):
        return False

    def requires(self):
        return PruneIndexes(collection_name=self.collection_name, data_folder=self.data_folder,
                            hadoop_jar=self.hadoop_jar, lucene_jar=self.lucene_jar, hadoop_bin=self.hadoop_bin)
