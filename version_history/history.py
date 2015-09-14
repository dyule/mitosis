from base64 import b64encode
from codecs import decode
import json
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
        result = self.connection.find("REVISION")
        # If there isn't one, then initialize the repository

        if len(result) < 1:
            self._intitialze_repo()

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
        self.connection.post(Statement('CREATE (b:BRANCH {name:"head"}) <-[:AT]- (r:REVISION)'))

    def create_file(self, **data):
        """
        Creates a new file creation command in the repository.  The file won't be created until the :meth:`commit`
        method is called.

        :param dict[str, any] data: The data associated with this file entity.  This includes filename, file type,
                                    parent folders, and anything else required.  This structure is up to the user
                                    of this class, this class only worries about storing the data
        :return: The temporary id of this file
        :rtype: str
        """
        def encode_bytes(b):
            return decode(b64encode(b), "ascii")
        new_id = "temp_" + str(self._max_id)
        self._max_id += 1
        statement = "CREATE (revision) <-[:OCCURRED]- (c_{0}:COMMAND {{command_{0}}}) " \
                    "-[:APPLIED_TO]-> (e_{0}:FILE_ENTITY {{entity_{0}}})".format(new_id)
        self._statements.append(statement)
        self._parameters["command_" + new_id] = {
            "type": "create",
            "data": json.dumps(data, separators=(",", ":"), sort_keys=True, default=encode_bytes),
        }
        self._parameters["entity_" + new_id] = {
        }
        return new_id

    def delete_file(self, file_id):
        statement = "CREATE (revision) <-[:OCCURRED]- (c_{0}:COMMAND {{command_{0}}}) " \
                    "-[:APPLIED_TO]-> (e_{0}) ".format(file_id)
        self._statements.append(statement)
        self._parameters['command_' + str(file_id)] = {
            'type': "delete",
        }
        self._lookup_ids.add(file_id)

    def modify_file(self, file_id, *operations):
        statement = "CREATE (revision) <-[:OCCURRED]- (c_{0}:COMMAND {{command_{0}}}) " \
                    "-[:APPLIED_TO]-> (e_{0}), (c_{0}) -[:FIRST_OP]-> ".format(file_id)
        operation_statements = ["(:OPERATION {{op_{0}_{1}}})".format(file_id, index) for index in range(len(operations))]
        self._parameters['command_' + str(file_id)] = {
            'type': "modify",
        }
        for operation_index in range(len(operations)):
            self._parameters['op_' + str(file_id) + '_' + str(operation_index)] = operations[operation_index]

        self._statements.append(statement + " -[:NEXT_OP]-> ".join(operation_statements))
        self._lookup_ids.add(file_id)

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
                           ["MATCH (e_{0}) WHERE id(e_{0}) = {0}".format(obj_id)
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
                                                 "OPTIONAL MATCH (old_rev) -[a:AT]-> (branch:BRANCH) "
                                                 "CREATE (branch) <-[:AT]- (n:REVISION) <-[:NEXT_COMMAND]- (old_rev) "
                                                 "DELETE a "
                                                 "RETURN id(n) "))
        mapping = {"temp_{}".format(i + 1): results[0]['data'][0]['row'][i] for i in range(self._max_id - 1)}
        self._statements = []
        self._parameters = {}
        self._max_id = 1
        self._lookup_ids = set()
        return results[1]['data'][0]['row'][0], mapping
