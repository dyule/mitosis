from version_history.connection import Connection


def before_scenario(context, scenario):
    if "database" in scenario.tags:
        context.connection = Connection('neo4j', 'password')
        context.connection.clear_database()
