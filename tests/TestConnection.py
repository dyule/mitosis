from io import StringIO
import unittest
from version_history.connection import Connection, Statement


def add_dummy_data(connection):
    create_statements = """
    CREATE p = (b:PERSON {name: "Bob", friendly: true}) -[:KNOWS]-> (a: PERSON {name: "Alice"}) -[:KNOWS]->
    (e:PERSON {name: "Eve"})
    return p
    """
    return connection.post(Statement(create_statements.strip()))


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.connection = Connection("neo4j", "password")
        self.connection.clear_database()

    def test_add_dummy_data(self):
        result = add_dummy_data(self.connection)
        self.assertEqual(result[0]['data'][0],  {'row': [[{'name': 'Bob', 'friendly': True}, {},
                                                          {'name': 'Alice'}, {},
                                                          {'name': 'Eve'}]]})

    def test_clear_database(self):
        add_dummy_data(self.connection)
        self.connection.clear_database()
        lookup_response = self.connection.post(Statement("MATCH (n) RETURN n LIMIT 100"))
        self.assertEqual(0, len(lookup_response[0]['data']))

    def test_wrong_syntax(self):
        self.assertRaises(ConnectionError, self.connection.post, Statement("CREATE (n) -> WHERE id(n)=5"))

    def test_post_multiple(self):
        add_dummy_data(self.connection)
        results = self.connection.post(Statement("MATCH (b {name: 'Bob'}) CREATE (n) -[:KNOWS]-> (b) return n"),
                                       Statement("MATCH (e {name: 'Eve'}) <-[r:KNOWS]- () DELETE e, r"))
        self.assertEqual(2, len(results))
        self.assertDictEqual({'data': [{'row': [{}]}], 'columns': ['n']}, results[0])
        self.assertDictEqual({'data': [], 'columns': []}, results[1])
        print(results)

    def test_find(self):
        add_dummy_data(self.connection)
        result = self.connection.find("PERSON")
        self.assertEqual(3, len(result))
        for person in result:
            self.assertIn(person[1]['name'], ["Alice", "Bob", "Eve"])

        result = self.connection.find("PERSON", {'name': "Eve"})
        self.assertEqual(1, len(result))
        self.assertEqual(result[0][1]['name'], "Eve")

        result = self.connection.find(match_params={'name': "Alice"})
        self.assertEqual(1, len(result))
        self.assertEqual(result[0][1]['name'], "Alice")

        result = self.connection.find(match_params={'name': 'Bob', 'friendly': True})
        self.assertEqual(1, len(result))
        self.assertEqual(result[0][1]['name'], "Bob")
        self.assertEqual(result[0][1]['friendly'], True)


class TestStatement(unittest.TestCase):
    def test_write_json(self):
        output = StringIO()
        Statement("MATCH (n) RETURN n LIMIT 100").write_json(output)
        self.assertEqual(output.getvalue(), '{"statement":"MATCH (n) RETURN n LIMIT 100"}')

        output = StringIO()
        Statement('CREATE (b:PERSON {name: "Bob"})').write_json(output)
        self.assertEqual(output.getvalue(), '{"statement":"CREATE (b:PERSON {name: \\"Bob\\"})"}')

        output = StringIO()
        Statement('CREATE p = (b:PERSON {name: "Bob"})\nreturn p').write_json(output)
        self.assertEqual(output.getvalue(), '{"statement":"CREATE p = (b:PERSON {name: \\"Bob\\"})\\nreturn p"}')

        output = StringIO()
        Statement("MATCH (n) RETURN n LIMIT {limit}", {'limit': 1}).write_json(output)
        self.assertEqual('{"statement":"MATCH (n) RETURN n LIMIT {limit}","parameters":{"limit":1}}',
                         output.getvalue())

        output = StringIO()
        Statement("CREATE ({ props })", {"props": {"position": "Developer", "name": "Andres"}}).write_json(output)
        self.assertEqual('{"statement":"CREATE ({ props })",' +
                         '"parameters":{"props":{"name":"Andres","position":"Developer"}}}',
                         output.getvalue())
