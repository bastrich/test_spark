package com.bastrich

import com.bastrich.utils.Task1BaseSpec

class Task1_1_Spec
  extends Task1BaseSpec {

  it("test enriching with sessions") {
    val task = new Task1_1
    testEnrichingWithSessions(task.enrichWithSessionIds)
  }

  it("test wrong input data schema") {
    val task = new Task1_1
    testWrongInputSchema(task.enrichWithSessionIds)
  }
}
