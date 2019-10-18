Classes:
* Task1 - Main-class with Task1
* Task1Sql - Main-class with Task1 - SQL-solution
* Task2a - Main-class with Task2:1st subparagraph
* Task2aSql - Main-class with Task2:1st subparagraph - SQL-solution
* Task2b - Main-class with Task2:2nd subparagraph
* Task2c - Main-class with Task2:3rd subparagraph
* Task1Spec - Unit-test for Task1

1. I never used sbt before, so here I just used IDEA generated sbt-project structure
2. There is a set of assumptions and unfinished parts as I didn't have enough time:
    1. percentile_approx is used in Task2a, so median will not be exact. I think, a good approach to calculate exact median is the following (24h-period is almost finished, so I don't have time to implement it):
       1. Sort values.
       2. Run 2 iterations over all the values: 1st is with step of length 1, 2nd is with step of length 2.
       3. When 2nd iteration is finished, we can take a median value from current position of 1st iteration.
       4. IMPORTANT! - the 1st iteration must have the same +-1 resulting amount of steps as the 2nd.
    2. Solution of Task2c is probably a little bit simplified and doesn't cover all the cases.
    3. Solution of Task1 likely doesn't cover all the cases as well. I think so based on my heuristic check as I didn't find enough time to check this exactly.
    4. Solutions with SQL implemented only for Task1 and Task2a. I'll try implement SQL-solution for other tasks, if I have enough time before before ending 24h-period. Generally, transforamtion "Spark Pipeline" <-> "SQL Query" is simple enough action.
    5. Test more or less implemented only for Task1. I'll try to implement tests for other tasks, if I have enough time before before ending 24h-period.
    6. In my opinion there are still a lot of non-optimal spark and spark-sql calls, but I ignore them right now in order to finish with all the tasks.
    7. The whole structure of project (inputs, outputs, verifying, readability, codestyle etc.) is not very pleasant, but I'll try to fix that as well, If I have enough time.