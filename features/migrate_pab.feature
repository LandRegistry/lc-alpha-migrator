@MigratePab

Feature: Migrate a PAB

Scenario: migrate a standard pab

Given I have inserted a row into the legacy db
When I submit a date range to the migrator
And I submit a date range to the legacy db
And it returns a 200 OK response
Then a new record is stored on the register database in the correct format
