import json
from behave import given, when, then
from hamcrest import assert_that, is_, is_in
from version_history.connection import Statement
from version_history.history import History


@given("An empty repository")
def empty_repository(context):
    context.repository = History(context.connection)
    context.first_rev = context.connection.find("REVISION")[0][0]


@when("I commit add commands")
def commit_add_commands(context):
    # Create a file, and a folder in the root directory
    file_id_a = context.repository.create_file(filename="File A", content=b"This is the file's content", type='file')
    dir_id_b = context.repository.create_file(filename="Directory B", type='directory')

    # Create two other files within the new folder
    file_id_c = context.repository.create_file(filename="File C", content=b"Some other content", type='file')
    file_id_d = context.repository.create_file(filename="File D", content=b"Still more content", type='file')

    # Create a directory inside the new directory and put a file in there
    dir_id_e = context.repository.create_file(filename="Directory E", type='directory')
    file_id_f = context.repository.create_file(filename="File F", content=b"Interior stuff", type='file')
    context.this_rev, mapping = context.repository.commit(context.first_rev)
    context.mapping = {
        "A": mapping[file_id_a],
        "B": mapping[dir_id_b],
        "C": mapping[file_id_c],
        "D": mapping[file_id_d],
        "E": mapping[dir_id_e],
        "F": mapping[file_id_f],
    }


@then("The repository contains the added file entities")
def check_for_added_files(context):

    def check_command_entity_association(cfile_id):
        c_result = context.connection.post(Statement("MATCH (e:FILE_ENTITY) <-[APPLIED_TO]- (c:COMMAND) "
                                                     "WHERE id(e) = {} RETURN c".format(context.mapping[cfile_id])))
        assert_that(len(c_result[0]['data']), is_(1), "Exactly one file data associated with " + file)

    entities = context.connection.find("FILE_ENTITY")
    assert_that(len(entities), is_(6), "We found 6 entities")

    # Make sure that the file entities are associated with one command object each
    for file in ["A", "B", "C", "D", "E", "F"]:
        check_command_entity_association(file)

    commands = context.connection.find("COMMAND")
    assert_that(len(commands), is_(6), "We found 6 commands")
    command_mapping = {}

    # Set up a mapping between filenames and commands operating on those files
    for command in commands:
        print(command[1]['data'])
        c_data = json.loads(command[1]['data'])
        if 'filename' in c_data:
            command_mapping[c_data['filename']] = command[1]
            command_mapping[c_data['filename']]['data'] = c_data

    # Verify that the data and type of each command is as expected
    assert_that(command_mapping["File A"]['data']['content'], is_("VGhpcyBpcyB0aGUgZmlsZSdzIGNvbnRlbnQ="))
    assert_that('content', not is_in(command_mapping["Directory B"]['data']))
    assert_that(command_mapping["File C"]['data']['content'], is_("U29tZSBvdGhlciBjb250ZW50"))
    assert_that('content', not is_in(command_mapping["File C"]['data']))
    assert_that(command_mapping["File D"]['data']['content'], is_("U3RpbGwgbW9yZSBjb250ZW50"))
    assert_that(command_mapping["File F"]['data']['content'], is_("SW50ZXJpb3Igc3R1ZmY="))

    assert_that(command_mapping["File A"]['data']['type'], is_("file"))
    assert_that(command_mapping["Directory B"]['data']['type'], is_("directory"))
    assert_that(command_mapping["File C"]['data']['type'], is_("file"))
    assert_that(command_mapping["File D"]['data']['type'], is_("file"))
    assert_that(command_mapping["Directory E"]['data']['type'], is_("directory"))
    assert_that(command_mapping["File F"]['data']['type'], is_("file"))

    assert_that(command_mapping["File A"]['type'], is_("create"))
    assert_that(command_mapping["Directory B"]['type'], is_("create"))
    assert_that(command_mapping["File C"]['type'], is_("create"))
    assert_that(command_mapping["File D"]['type'], is_("create"))
    assert_that(command_mapping["Directory E"]['type'], is_("create"))
    assert_that(command_mapping["File F"]['type'], is_("create"))

    # Ensure that the commands are associated with the previous revision id
    commands_associated = context.connection.post(Statement("MATCH (c:COMMAND) -[:OCCURRED]-> "
                                                            "(r:REVISION) WHERE id(r) = {} RETURN c"
                                                  .format(context.first_rev)))
    assert_that(len(commands_associated[0]['data']), is_(6), "We found 6 commands")

    # Make sure that the commit updated the current revision and left nothing behind.
    result = context.connection.post(Statement("MATCH (:HEAD) <-[:AT]- (r:REVISION) return r"))

    assert_that(len(result[0]['data']), is_(1), "There was only one current revision")

    result = context.connection.post(Statement("MATCH (:HEAD) <-[:AT]- (r:REVISION) -[:APPLIED_TO]-> (n) return n"))

    assert_that(len(result[0]['data']), is_(0), "The new revision has no commands associated with it")


@given("A connection to the database")
def database_connection(context):
    # Done by the database tag
    pass


@when("I create a repository")
def create_repository(context):
    """
    :type context behave.runner.Context
    """
    context.repository = History(context.connection)


@then("The repository should be correctly initialized")
def repository_is_initialized(context):
    """
    :type context behave.runner.Context
    """
    result = context.connection.post(Statement("MATCH (r:REVISION) return r, id(r)"),
                                     Statement("MATCH (h:HEAD) return h, id(h)"),
                                     Statement("MATCH (n) return n"))

    assert_that(len(result), is_(3), "We found both the first revision and the root file entity")
    assert_that(len(result[0]['data']), is_(1), "There was only one revision")
    assert_that(len(result[1]['data']), is_(1), "There was only one head")
    assert_that(len(result[2]['data']), is_(2), "There was only two nodes: the revision and the head")



@given("A repository with some files in in")
def repository_with_files(context):
    """
    :type context behave.runner.Context
    """
    context.execute_steps("""
    Given An empty repository
    When I commit add commands
    """)


@when("I commit delete commands")
def commit_delete_commands(context):
    """
    :type context behave.runner.Context
    """
    context.repository.delete_file(context.mapping["A"])
    context.repository.delete_file(context.mapping["F"])
    context.old_rev = context.this_rev
    context.this_rev, _ = context.repository.commit(context.this_rev)


@then("The repository records that the file entities have been deleted")
def check_for_deleted_files(context):
    """
    :type context behave.runner.Context
    """
    commands = context.connection.post(Statement("MATCH (r) <-- (c:COMMAND) --> (e:FILE_ENTITY) "
                                                 "<-- (c1:COMMAND) where id(r) = {} return c, c1"
                                                 .format(context.old_rev)))[0]['data']

    assert_that(len(commands), is_(2))
    assert_that(commands[0]['row'][0]['type'], is_("delete"), "We associated a delete command with the revision")
    assert_that(commands[1]['row'][0]['type'], is_("delete"), "We associated a delete command with the revision")

    assert_that(commands[0]['row'][1]['type'], is_("create"), "The delete command was preceded by a create command")
    assert_that(commands[1]['row'][1]['type'], is_("create"), "The delete command was preceded by a create command")

    data = {commands[0]['row'][1]['data'], commands[1]['row'][1]['data']}

    assert_that(data, is_({'{"content":"SW50ZXJpb3Igc3R1ZmY=","filename":"File F","type":"file"}',
                           '{"content":"VGhpcyBpcyB0aGUgZmlsZSdzIGNvbnRlbnQ=","filename":"File A","type":"file"}'}))
