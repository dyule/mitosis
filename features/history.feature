Feature: Commiting Changes
  As a user
  I want to commit operations to the database
  So that I can record the changes to my file system
  @database
  Scenario: Setting up repository
    Given A connection to the database
    When I create a repository
    Then The repository should be correctly initialized

  @database
  Scenario: Committing Add Commands
    Given An empty repository
    When I commit add commands
    Then The repository contains the added file entities

  @database
  Scenario: Committing Delete Commands
    Given A repository with some files in in
    When I commit delete commands
    Then The repository records that the file entities have been deleted