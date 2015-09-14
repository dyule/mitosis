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
    file_id_a = context.repository.create_file(context.repository.root_entity_id, "File A",
                                               b"This is the file's content")
    dir_id_b = context.repository.create_directory(context.repository.root_entity_id, "Directory B")

    # Create two other files within the new folder
    file_id_c = context.repository.create_file(dir_id_b, "File C", b"Some other content")
    file_id_d = context.repository.create_file(dir_id_b, "File D", b"Still more content")

    # Create a directory inside the new directory and put a file in there
    dir_id_e = context.repository.create_directory(dir_id_b, "Directory E")
    file_id_f = context.repository.create_file(dir_id_e, "File F", b"Interior stuff")
    context.this_rev, mapping = context.repository.commit(context.first_rev)
    context.mapping = {
        "A": mapping[file_id_a],
        "B": mapping[dir_id_b],
        "C": mapping[file_id_c],
        "D": mapping[file_id_d],
        "E": mapping[dir_id_e],
        "F": mapping[file_id_f],
    }


@then("The repository contains the added files")
def check_for_added_files(context):

    def check_data_entity_association(file):
        result = context.connection.post(Statement("MATCH (e:FILE_ENTITY) <-[INSTANCE_OF]- (d:FILE_DATA) "
                                                   "WHERE id(e) = {} RETURN d".format(context.mapping[file])))
        assert_that(len(result[0]['data']), is_(1), "Exactly one file data associated with " + file)

    def check_command_entity_association(file):
        result = context.connection.post(Statement("MATCH (e:FILE_ENTITY) <-[APPLIED_TO]- (c:COMMAND) "
                                                   "WHERE id(e) = {} RETURN c".format(context.mapping[file])))
        assert_that(len(result[0]['data']), is_(1), "Exactly one file data associated with " + file)

    entities = context.connection.find("FILE_ENTITY")
    assert_that(len(entities), is_(7), "We found 7 entities (the root and the 6 created files)")

    # Set up a mapping between file names and the entity objects from the database
    entity_mapping = {}
    for file, file_id in context.mapping.items():
        for entity in entities:
            if file_id == entity[0]:
                entity_mapping[file] = entity[1]

    # Verify that the entity objects in the database are the right ones
    assert_that(entity_mapping["A"]['type'], is_("file"))
    assert_that(entity_mapping["B"]['type'], is_("folder"))
    assert_that(entity_mapping["C"]['type'], is_("file"))
    assert_that(entity_mapping["D"]['type'], is_("file"))
    assert_that(entity_mapping["E"]['type'], is_("folder"))
    assert_that(entity_mapping["F"]['type'], is_("file"))

    datas = context.connection.find("FILE_DATA")
    assert_that(len(datas), is_(7), "We found 7 file data nodes")

    # Set up a mapping between filenames and the data objects from the database
    data_mapping = {}
    for data in datas:
        if 'file_name' in data[1]:
            data_mapping[data[1]['file_name']] = data[1]

    # Verify that the data objects have the expected data in them
    assert_that(data_mapping["File A"]['data'], is_("VGhpcyBpcyB0aGUgZmlsZSdzIGNvbnRlbnQ="))
    assert_that('data', not is_in(data_mapping["Directory B"]))
    assert_that(data_mapping["File C"]['data'], is_("U29tZSBvdGhlciBjb250ZW50"))
    assert_that('data', not is_in(data_mapping["File C"]))
    assert_that(data_mapping["File D"]['data'], is_("U3RpbGwgbW9yZSBjb250ZW50"))
    assert_that(data_mapping["File F"]['data'], is_("SW50ZXJpb3Igc3R1ZmY="))

    # Make sure that the file entities are associated with one data object and one command object each
    for file in ["A", "B", "C", "D", "E", "F"]:
        check_data_entity_association(file)
        check_command_entity_association(file)

    commands = context.connection.find("COMMAND")
    assert_that(len(commands), is_(6), "We found 6 commands")
    command_mapping = {}

    # Set up a mapping between filenames and commands operating on those files
    for command in commands:
        if 'file_name' in command[1]:
            command_mapping[command[1]['file_name']] = command[1]

    # Verify that the data and type of each command is as expected
    assert_that(command_mapping["File A"]['data'], is_("VGhpcyBpcyB0aGUgZmlsZSdzIGNvbnRlbnQ="))
    assert_that('data', not is_in(command_mapping["Directory B"]))
    assert_that(command_mapping["File C"]['data'], is_("U29tZSBvdGhlciBjb250ZW50"))
    assert_that('data', not is_in(command_mapping["File C"]))
    assert_that(command_mapping["File D"]['data'], is_("U3RpbGwgbW9yZSBjb250ZW50"))
    assert_that(command_mapping["File F"]['data'], is_("SW50ZXJpb3Igc3R1ZmY="))

    assert_that(command_mapping["File A"]['type'], is_("create_file"))
    assert_that(command_mapping["Directory B"]['type'], is_("create_folder"))
    assert_that(command_mapping["File C"]['type'], is_("create_file"))
    assert_that(command_mapping["File D"]['type'], is_("create_file"))
    assert_that(command_mapping["Directory E"]['type'], is_("create_folder"))
    assert_that(command_mapping["File F"]['type'], is_("create_file"))

    # Ensure that the commands are associated with the previous revision id
    commands_associated = context.connection.post(Statement("MATCH (c:COMMAND) -[:OCCURRED]-> "
                                                            "(r:REVISION) WHERE id(r) = {} RETURN c"
                                                  .format(context.first_rev)))
    assert_that(len(commands_associated[0]['data']), is_(6), "We found 6 commands")


    # Verify that the file structure is correctly represented
    first_level = context.connection.post(Statement("MATCH (d1:FILE_DATA) <-[:CONTAINED]- (d:FILE_DATA) "
                                                    "-[:INSTANCE_OF]-> (e:FILE_ENTITY) WHERE id(e) = {} return d1"
                                                    .format(context.repository.root_entity_id)))
    file_collection = set()
    assert_that(len(first_level[0]['data']), is_(2), "Two objects at the first level")
    for file in first_level[0]['data']:
        file_collection.add(file['row'][0]['file_name'])

    second_level = context.connection.post(Statement("MATCH (d2: FILE_DATA) <-[:CONTAINED]- "
                                                     "(d1:FILE_DATA) <-[:CONTAINED]- (d:FILE_DATA) "
                                                     "-[:INSTANCE_OF]-> (e:FILE_ENTITY) WHERE id(e) = {} return d2"
                                                     .format(context.repository.root_entity_id)))
    assert_that(len(second_level[0]['data']), is_(3), "Three objects at the second level")
    for file in second_level[0]['data']:
        file_collection.add(file['row'][0]['file_name'])

    third_level = context.connection.post(Statement("MATCH (d3: FILE_DATA) <-[:CONTAINED]- "
                                                    "(d2:FILE_DATA) <-[:CONTAINED]-  "
                                                    "(d1:FILE_DATA) <-[:CONTAINED]- (d:FILE_DATA) "
                                                    "-[:INSTANCE_OF]-> (e:FILE_ENTITY) WHERE id(e) = {} return d3"
                                                    .format(context.repository.root_entity_id)))
    assert_that(len(third_level[0]['data']), is_(1), "Three objects at the second level")
    for file in third_level[0]['data']:
        file_collection.add(file['row'][0]['file_name'])
    assert_that(file_collection, is_({'File C', 'File F', 'File D', 'Directory B', 'File A', 'Directory E'}),
                "All files arranged in the hierarchy")

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
                                     Statement("MATCH (r:FILE_ENTITY) return r, id(r)"),
                                     Statement("MATCH (r:FILE_ENTITY) <-[:INSTANCE_OF]- (s:FILE_DATA) return (id(r))"))

    assert_that(len(result), is_(3), "We found both the first revision and the root file entity, and the file data")
    assert_that(len(result[0]['data']), is_(1), "There was only one revision")

    assert_that(len(result[1]['data']), is_(1), "There was only one file entity")
    assert_that(result[1]['data'][0]['row'][0]['is_root'], is_(True), "The entity is the root")
    entity_id = result[1]['data'][0]['row'][1]
    assert_that(context.repository.root_entity_id, is_(entity_id), "The repository saved the correct entity id")

    assert_that(len(result[2]['data']), is_(1), "There exactly one file entity with file data attached")
    entity_id = result[2]['data'][0]['row'][0]
    assert_that(context.repository.root_entity_id, is_(entity_id), "The repository saved the correct entity id")


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
    # context.repository.delete_file(context.mapping["A"])
    context.repository.delete_folder(context.mapping["E"])
    #context.this_rev, _ = context.repository.commit(context.this_rev)


@then("The repository no longer contains the deleted files")
def check_for_deleted_files(context):
    """
    :type context behave.runner.Context
    """
    pass