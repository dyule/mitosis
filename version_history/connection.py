from base64 import b64encode
from codecs import decode
import json
from io import StringIO
import requests


class Connection:
    def __init__(self, username, password, host='localhost', port=7474, path='db/data'):
        """
        Initializes a connection to the graph database.  No requests will be made until one of the methods are called

        :param str username: The username to use for connecting to the database.  Required
        :param str password: The password for connecting to the database.  Required
        :param str host: The hostname where the database is located.  Defaults to 'localhost'
        :param int port: The port the database is listening on. Defaults to 7474.
        :param str path: The path the database is located at. Used in case of multiple databases. Defaults to 'db/data'
        """
        self._path = path
        self._port = port
        self._host = host
        self._password = password
        self._username = username

        self._url = "http://{}:{}/{}/transaction/commit".format(host, port, path)

    def clear_database(self):
        """
        Empties the database of all nodes and relationships.

        :return: The result of the request
        :rtype: dict[str, dict]
        """
        return self.post(Statement("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r"))[0]

    def find(self, label=None, match_params=None):
        """
        Finds nodes matching the given criteria.  The results are returned as an array of tuples,
        with each tuple consisting of the id of the node and a dict containing its properties.  For example::

            [(15, {"name": "Bob"}),
             (37, {"name": "Alice", "friendly": True})]


        :param str label: A label that matching nodes must have (optional)
        :param dict match_params: The properties that matching nodes must have
        :return: An array containing the results of the find
        :rtype: list[(int, dict)]
        """
        output = StringIO()
        output.write("MATCH (r")
        if label:
            output.write(":")
            output.write(label)
        output.write(") ")
        if match_params:
            output.write(" WHERE ")
            is_first = True
            for key, value in match_params.items():
                if not is_first:
                    output.write(" AND ")
                output.write("r.")
                output.write(key)
                output.write(' = ')
                if isinstance(value, str):
                    output.write('"')
                    output.write(value)
                    output.write('"')
                else:
                    output.write(str(value))
                is_first = False

        output.write(" return r, id(r)")
        return [(row['row'][1], row['row'][0]) for row in self.post(Statement(output.getvalue()))[0]['data']]

    def post(self, *statements):
        """
        Sends statements to the database to be executed as a single transaction. The results are returned as delivered
        from the database.  For example::

            [
              {
                "data":[
                  {
                    "row":[
                      {
                        "friendly": True,
                        "name": "Bob"
                      },
                      15
                    ]
                  },
                  {
                    "row":[
                      {
                        "name": "Alice"
                      },
                      16
                    ]
                  }
                ],
                "columns":["r", "id(r)"]
              }
            ]

        :param statements: The statements to be executed on the database
        :type statements: list[:class:`Statement`]
        :return: The result of executing the statements on the database
        :rtype: list[dict[str, list[dict[str, any]]
        """
        statement_body = StringIO()
        statement_body.write('{"statements":[')
        for index in range(len(statements) - 1):
            statements[index].write_json(statement_body)
            statement_body.write(",")
        statements[len(statements) - 1].write_json(statement_body)
        statement_body.write(']')
        result = requests.post(self._url, statement_body.getvalue(), auth=(self._username, self._password)).json()
        if len(result['errors']) > 0:
            print(result['errors'][0]['message'])
            raise ConnectionError(result['errors'][0]['message'])
        return result['results']


class Statement:
    __slots__ = ['statement', 'parameters']

    def __init__(self, statement, parameters=None):
        """
        Generates a statement that can be executed on the server.  Lightweight class.

        :param str statement: The statement to be executed
        :param dict[str, any] parameters: An array of parameters to be transmitted along with the statement.
                                          Each parameter should be referenced in the statement by its key
        """
        self.statement = statement
        self.parameters = parameters

    def write_json(self, writer):
        """
        Writes the compact json representation of the statement to a file like object

        :param writer: A file like object
        """
        def write_dict(d):
            writer.write('{')
            first = True
            for k, v in d.items():
                if first:
                    first = False
                else:
                    writer.write(",")
                writer.write("\\\"")
                writer.write(k)
                writer.write("\\\":")
                if v is None:
                    writer.write("null")
                elif isinstance(v, str):
                    writer.write("\\\"")
                    writer.write(v)
                    writer.write("\\\"")
                elif isinstance(v, bool) or isinstance(v, int):
                    writer.write(v)
                elif isinstance(v, bytes) or isinstance(v, bytearray):
                    writer.write("\\\"")
                    writer.write(decode(b64encode(v), "ascii"))
                    writer.write("\\\"")
                elif isinstance(v, dict):
                    write_dict(v)
            writer.write('}')
        writer.write('{"statement":"')
        writer.write(self.statement.replace('"', '\\"').replace("\n", "\\n"))
        writer.write('"')
        if self.parameters:
            writer.write(',"parameters":')
            writer.write(json.dumps(self.parameters, separators=(',', ':'), sort_keys=True))
        writer.write('}')



