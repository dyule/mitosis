Node Types:
----------

 - File Entity
 - File Data
 - File Operation
 - Revision

Relationship Types
----------

 - Contained (data - data)
 - Occurred (revision - command)
 - Applied To (command - entity)
 - Instance Of (data - Entity)
 - Next Command (command - command)
 - Next Op(operation - operation)
 - Next Rev (rev - rev)
 - Part Of (operation - command)

Command Types
----------

 - Create
 - Delete
 - Update

For CREATE
----------

 - create a new entity
 - create a new command
 - create a new data
 - associate entity with data via instance of
 - associate command with current revision via occurred
 - associate command with entity via applied to
 - associate data with its parent via contained

For DELETE
----------

 - create new command
 - associate command with current revision via occurred
 - associate command with most recent operation associated with the entity via next op
 - associate command with entity via applied to
 - delete file data
 - delete instance of relationship

For UPDATE
----------

 - create new command
 - create constituent operations
 - associate operations in order via Next Op
 - associate command with current revision via occurred
 - associate command with most recent operation associated with the entity via next op
 - associate command with entity via applied to
 - associate first operation with command
 - update file data
 - update contained?


Operation Types
----------

 - Insert
 - Remove
