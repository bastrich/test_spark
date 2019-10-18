Classes:
* Task1 - Main-class with Task1
* Task1Sql - Main-class with Task1 - SQL-solution
* Task2a - Main-class with Task2:1st subparagraph
* Task2b - Main-class with Task2:2nd subparagraph
* Task2c - Main-class with Task3:3rd subparagraph
* Task1Spec - Unit-test for Task1

1. I never used sbt before, so here I just used IDEA generated sbt-project structure
2. There is a set of assumptions and unfinished parts as I didn't have enough time:
    1. percentile_approx is used in Task2a, so median will not be exact. I think, a good approach to calculate exact median is the following (24h-period is almost finished, so I don't have time to implement it):
       1. Sort values.
       2. Run 2 iterations over all the values: 1st is with step of length 1, 2nd is with step of length 2.
       3. When 2nd iteration is finished, we can take a median value from current position of 1st iteration.
       4. IMPORTANT! - the 1st iteration must have the same +-1 resulting amount of steps as the 2nd.
    2. Solution of Task2c is probably a little bit simplified and doesn't cover all the cases.
    3. Solutions with SQL implemented only for Task1. I'll try implement SQL-solution for other tasks, if I have enough time before before ending 24h-period. Generally, transforamtion "Spark Pipeline" <-> "SQL Query" is simple enough action.
    4. Test more or less implemented only for Task1. I'll try to implement tests for other tasks, if I have enough time before before ending 24h-period.
    5. The whole structure of project (inputs, outputs, verifying, readability, codestyle etc.) is not very pleasant, but I'll try to fix that as well, If I have enough time.