Classes:
* Task1 - Main-class with Task1
* Task1Sql - Main-class with Task1 - SQL-solution
* Task2a - Main-class with Task2:1st subparagraph
* Task2b - Main-class with Task2:2nd subparagraph
* Task2c - Main-class with Task3:3rd subparagraph

1. I never used sbt before, so here I just used IDEA generated sbt-project structure
2. There is a set of assumptions and unfinished parts as I didn't have enough time:
    1. percentile_approx is used in Task2a, so median will not be exact.
    2. Solution of Task2c is probably a little bit simplified and doesn't cover all the cases.
    3. Solutions with SQL implemented only for Task1. I'll try implement SQL-solution for other tasks, if I have enough time before before ending 24h-period. Generally, transforamtion "Spark Pipeline" <-> "SQL Query" is simple enough action.
    4. There are no tests yet. I'll try to implement them, if I have enough time before before ending 24h-period.
    5. The whole structure of project (inputs, outputs, verifying, readability, codestyle etc.) is not very pleasant, but I'll try to fix that as well, If I have enough time.