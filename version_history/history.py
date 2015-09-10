from base64 import b64encode
from codecs import decode
from version_history.connection import Statement


class History:
    def __init__(self, connection):
        """
        Set up versioning for a file tree.  This class is not thread safe, nor is it designed for concurrent
        access.  The database behind it, however, is.

        :param connection: The connection to the database that this repository will use
        :type connection: :class:`version_history.connection.Connection`
        """
        self.connection = connection
        # Find the root file entity
        result = self.connection.find("FILE_ENTITY", {'is_root': True})
        # If there isn't one, then initialize the repository

        if len(result) != 1:
            self._intitialze_repo()
        else:
            self.root_entity_id = result[0][0]

        #: The statements that will update the database to reflect the commands so far this revision
        self._statements = []
        #: The parameters associated with the statements for this revision
        self._parameters = {}
        self._max_id = 1

        #: The ids that need to be looked up for reference within statements
        self._lookup_ids = set()

    def _intitialze_repo(self):
        """
        Create the repository in the database.  Creates a starting revision and root file entity and data.
        """
        result = self.connection.post(Statement('CREATE (h:HEAD) <-[:AT]- (r:REVISION),'
                                                '(e:FILE_ENTITY {is_root: true, type: "root"}) '
                                                '<-[:INSTANCE_OF]- (d:FILE_DATA {data: ""})'
                                                'RETURN id(e)'))
        self.root_entity_id = result[0]['data'][0]['row'][0]

    def create_file(self, parent_id, filename, content):
        """
        Creates a new file creation command in the repository.  The file won't be created until the :meth:`commit`
        method is called.

        :param string|int parent_id: The ID of the parent of this file
        :param filename: The name of this file
        :param content: The content of this file
        :return: The temporary id of this file
        :rtype: str
        """
        new_id = "temp_" + str(self._max_id)
        self._max_id += 1
        if not str(parent_id).startswith("temp"):
            self._lookup_ids.add(parent_id)
        statement = "CREATE (revision) <-[:OCCURRED]- (c_{0}:COMMAND {{command_{0}}}) " \
                    "-[:APPLIED_TO]-> (e_{0}:FILE_ENTITY {{entity_{0}}}) " \
                    "<-[:INSTANCE_OF]- (d_{0}:FILE_DATA {{data_{0}}}) " \
                    "<-[:CONTAINED]- (d_{1})".format(new_id, parent_id)
        data = decode(b64encode(content), "ascii")
        self._statements.append(statement)
        self._parameters["command_" + new_id] = {
            "type": "create_file",
            "file_name": filename,
            "data": data
        }
        self._parameters["entity_" + new_id] = {
            "type": "file",
        }
        self._parameters["data_" + new_id] = {
            "file_name": filename,
            "data": data,
        }
        return new_id

    def create_directory(self, parent_id, filename):
        """
        Creates a new directory creation command in the repository.  The directory won't be created until the :meth:`commit`
        method is called.

        :param string|int parent_id: The ID of the parent of this directory
        :param filename: The name of this directory
        :return: The temporary id of this directory
        :rtype: str
        """

        new_id = "temp_" + str(self._max_id)
        self._max_id += 1
        if not str(parent_id).startswith("temp"):
            self._lookup_ids.add(parent_id)
        statement = "CREATE (revision) <-[:OCCURRED]- (c_{0}:COMMAND {{command_{0}}}) " \
                    "-[:APPLIED_TO]-> (e_{0}:FILE_ENTITY {{entity_{0}}}) " \
                    "<-[:INSTANCE_OF]- (d_{0}:FILE_DATA {{data_{0}}}) " \
                    "<-[:CONTAINED]- (d_{1})".format(new_id, parent_id)
        self._statements.append(statement)
        self._parameters["command_" + new_id] = {
            "type": "create_folder",
            "file_name": filename,
        }
        self._parameters["entity_" + new_id] = {
            "type": "folder",
        }
        self._parameters["data_" + new_id] = {
            "file_name": filename,
        }
        return new_id

    def delete_file(self, file_id):
        statement = "MATCH (d_{0}) -[r]- () " \
                    "CREATE (revision) <-[:OCCURRED]- (c_{0}:COMMAND {{command_{0}}}) " \
                    "-[:APPLIED_TO]-> (e_{0}) " \
                    "DELETE r, d_{0}".format(file_id)
        self._statements.append(statement)
        self._parameters['command_' + str(file_id)] = {
            'type': "delete_file",
        }
        self._lookup_ids.add(file_id)

    def delete_folder(self, folder_id):
        statement = "CREATE (revision) <-[:OCCURRED]- (c_{0}:COMMAND {{command_{0}}}) WITH " \
                    "MATCH path = (d_{0}) -[:CONTAINED*]-> (:FILE_DATA) " \
                    "-[:APPLIED_TO]-> (e_{0}) " \
                    "UNWIND nodes(path) AS n_{0} " \
                    "MATCH n_{0} -[r]- () " \
                    "DELETE n_{0}, r".format(folder_id)

        self._statements.append(statement)
        self._parameters['command_' + str(folder_id)] = {
            'type': "delete_folder",
        }
        self._lookup_ids.add(folder_id)

    def commit(self, parent_revision):
        """
        Commit all commands that have been created so far.  Finishes this revision and advances to the next one

        :param int parent_revision: The id of the revision that this commit is operating on
        :return: The id of the revision just committed and
                 a dictionary of temporary ids to their actual id for access in the application
        :rtype: (int, dict[str, int])
        """
        # TODO make sure that there have been commands performed, or don't do anything
        match_statements = ["MATCH (revision:REVISION) WHERE id(revision) = {} ".format(parent_revision)] + \
                           ["MATCH (e_{0}) <-[:INSTANCE_OF]- (d_{0}) WHERE id(e_{0}) = {0}".format(obj_id)
                            for obj_id in self._lookup_ids]
        return_clauses = ["id(e_temp_{})".format(new_id) for new_id in range(1, self._max_id)]
        merged_statement = "\n".join(match_statements + self._statements)
        if len(return_clauses):
            merged_statement += "\nRETURN " + ",".join(return_clauses)
        print("----")
        print(merged_statement)
        results = self.connection.post(Statement(merged_statement, self._parameters),
                                       Statement("MATCH (old_rev:REVISION)  WHERE id(old_rev) = {} "
                                                 .format(parent_revision) +
                                                 "OPTIONAL MATCH (old_rev) -[a:AT]-> (head:HEAD) "
                                                 "CREATE (head) <-[:AT]- (n:REVISION) <-[:NEXT_COMMAND]- (old_rev) "
                                                 "DELETE a "
                                                 "RETURN id(n) "))
        mapping = {"temp_{}".format(i + 1): results[0]['data'][0]['row'][i] for i in range(self._max_id - 1)}
        self._statements = []
        self._parameters = {}
        self._max_id = 1
        self._lookup_ids = set()
        return results[1]['data'][0]['row'][0], mapping
